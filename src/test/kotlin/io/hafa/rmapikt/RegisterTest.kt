package io.hafa.rmapikt

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import mockwebserver3.MockResponse
import mockwebserver3.MockWebServer
import okio.Buffer
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class RegisterTest {
    private val server = MockWebServer().apply { start() }
    private val authHost get() = server.url("/").toString().removeSuffix("/")
    // see ClientTest: a per-test client, so no connection pool outlives its server
    private val http = okhttp3.OkHttpClient()
    private val emptyCacheDump =
        """{"version":2,"encoding":"base64","bodies":{},"exists":[]}"""

    @AfterTest
    fun shutdown() {
        server.close()
        http.connectionPool.evictAll()
        http.dispatcher.executorService.shutdown()
    }

    @Test
    fun `register exchanges a connect code for a device token`() = runTest {
        server.enqueue(MockResponse(code = 200, body = "device-token"))

        val token = register("abcdefgh", authHost = authHost, httpClient = http)
        assertEquals(DeviceToken("device-token"), token)

        val request = server.takeRequest()
        assertEquals("/token/json/2/device/new", request.target)
        assertEquals("POST", request.method)
        assertEquals("Bearer", request.headers["Authorization"], "registration is unauthenticated")

        val body = Json.parseToJsonElement(request.body?.utf8().orEmpty()) as JsonObject
        assertEquals(JsonPrimitive("abcdefgh"), body["code"])
        assertEquals(JsonPrimitive("browser-chrome"), body["deviceDesc"])
        assertTrue(
            Regex("^[0-9a-f-]{36}$").matches((body["deviceID"] as JsonPrimitive).content),
            "a device id is generated when none is supplied",
        )
    }

    @Test
    fun `register sends the device description and id it was given`() = runTest {
        server.enqueue(MockResponse(code = 200, body = "device-token"))

        register(
            "abcdefgh",
            deviceDesc = DeviceDescription.MobileAndroid,
            uuid = "11111111-2222-3333-4444-555555555555",
            authHost = authHost,
            httpClient = http,
        )
        val body = Json.parseToJsonElement(server.takeRequest().body?.utf8().orEmpty()) as JsonObject
        assertEquals(JsonPrimitive("mobile-android"), body["deviceDesc"])
        assertEquals(JsonPrimitive("11111111-2222-3333-4444-555555555555"), body["deviceID"])
    }

    @Test
    fun `a connect code of the wrong length is refused before any request`() = runTest {
        assertFailsWith<IllegalArgumentException> { register("short", authHost = authHost, httpClient = http) }
        assertEquals(0, server.requestCount, "no point asking the server about a malformed code")
    }

    @Test
    fun `a rejected registration reports the status and body`() = runTest {
        server.enqueue(MockResponse(code = 400, body = "code expired"))

        val error = assertFailsWith<ResponseException> { register("abcdefgh", authHost = authHost, httpClient = http) }
        assertEquals(400, error.status)
        assertEquals("code expired", error.body)
    }

    @Test
    fun `auth exchanges the device token for a session token`() = runTest {
        server.enqueue(MockResponse(code = 200, body = "session-token"))

        assertEquals(
            SessionToken("session-token"),
            auth(DeviceToken("device-token"), authHost = authHost, httpClient = http),
        )

        val request = server.takeRequest()
        assertEquals("/token/json/2/user/new", request.target)
        assertEquals("Bearer device-token", request.headers["Authorization"])
    }

    @Test
    fun `a rejected device token is reported`() = runTest {
        server.enqueue(MockResponse(code = 401, body = "unauthorized"))

        val error = assertFailsWith<ResponseException> {
            auth(DeviceToken("stale"), authHost = authHost, httpClient = http)
        }
        assertEquals(401, error.status)
    }

    @Test
    fun `session builds a client without touching the network`() {
        val client = session(SessionToken("session-token"), SessionOptions(rawHost = authHost, httpClient = http))
        assertEquals(0, server.requestCount, "building a client from a session token is offline")
        assertEquals(emptyCacheDump, Buffer().also { client.dumpCache(it) }.readUtf8())
    }

    @Test
    fun `remarkable authenticates once and then builds the client`() = runTest {
        server.enqueue(MockResponse(code = 200, body = "session-token"))

        remarkable(
            DeviceToken("device-token"),
            SessionOptions(authHost = authHost, rawHost = authHost, httpClient = http),
        )
        assertEquals(1, server.requestCount, "building the client costs exactly one auth call")
        assertEquals("/token/json/2/user/new", server.takeRequest().target)
    }

    @Test
    fun `the default hosts are the production ones`() {
        val options = SessionOptions()
        assertEquals("https://eu.tectonic.remarkable.com", options.rawHost)
        assertEquals("https://internal.cloud.remarkable.com", options.uploadHost)
        assertEquals("https://webapp-prod.cloud.remarkable.engineering", Hosts.AUTH)
        assertEquals(10, options.maxGenerationRetries)
        assertEquals(3, options.maxTransientRetries)
    }

    @Test
    fun `a client knows the id it registered under`() {
        // a jwt whose payload is {"device-id":"5bd526e8-a264-4e7b-ac82-78fbd72960b8"}
        val token = SessionToken(
            "header.eyJkZXZpY2UtaWQiOiI1YmQ1MjZlOC1hMjY0LTRlN2ItYWM4Mi03OGZiZDcyOTYwYjgifQ.sig",
        )
        assertEquals(DeviceId("5bd526e8-a264-4e7b-ac82-78fbd72960b8"), deviceIdOf(token))
        assertEquals(
            DeviceId("5bd526e8-a264-4e7b-ac82-78fbd72960b8"),
            session(token, SessionOptions(httpClient = http)).deviceId,
        )
    }

    @Test
    fun `a token that names no device says so rather than guessing`() {
        val notJwt = assertFailsWith<ValidationException> { deviceIdOf(SessionToken("opaque")) }
        assertEquals("the session token is not a jwt, so it names no device", notJwt.message)
        // {"sub":"someone"}, then a payload that is not json at all
        for (payload in listOf("eyJzdWIiOiJzb21lb25lIn0", "bm90IGpzb24")) {
            val nameless = assertFailsWith<ValidationException> {
                deviceIdOf(SessionToken("header.$payload.sig"))
            }
            assertEquals("the session token carries no device-id claim", nameless.message)
        }
    }

    @Test
    fun `every device description has the wire value the cloud expects`() {
        assertEquals(
            listOf(
                "desktop-windows", "desktop-macos", "desktop-linux",
                "mobile-android", "mobile-ios", "browser-chrome", "remarkable",
            ),
            DeviceDescription.entries.map {
                Json.parseToJsonElement(encodeWire(DeviceDescription.serializer(), it))
                    .jsonPrimitive.content
            },
        )
    }
}
