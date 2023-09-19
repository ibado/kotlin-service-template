package com.ibado

import com.github.jasync.sql.db.SuspendingConnection
import com.github.jasync.sql.db.asSuspending
import com.github.jasync.sql.db.pool.ConnectionPool
import com.github.jasync.sql.db.postgresql.PostgreSQLConnection
import com.github.jasync.sql.db.postgresql.PostgreSQLConnectionBuilder
import io.ktor.http.HttpStatusCode.Companion.BadRequest
import io.ktor.http.HttpStatusCode.Companion.NotFound
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.cio.*
import io.ktor.server.engine.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.future.await
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

fun main() {
    val connectionPool = createConnectionPool()
    val getUsers = resolveGetUsers(connectionPool)
    embeddedServer(CIO, port = 8080, host = "0.0.0.0") { module(getUsers) }
        .withGracefulShutdown(connectionPool)
        .start(true)
}

fun Application.module(getUser: suspend (Int) -> User?) {
    configureJsonSerialization()
    routing {
        get("/users/{id}") {
            when (val userId = call.parameters["id"]?.toIntOrNull()) {
                null -> call.respond(BadRequest, "A valid id must be provided.")
                else -> getUser(userId)
                    ?.let { user -> call.respond(user) }
                    ?: call.respond(NotFound, "User not found.")
            }
        }
    }
}

@Serializable
data class User(val id: Int)

private fun resolveGetUsers(
    connectionPool: ConnectionPool<PostgreSQLConnection>
): suspend (Int) -> User? = { id ->
    connectionPool.acquire()
        .sendQuery("SELECT * FROM users WHERE id = $id;")
        .rows
        .firstOrNull()
        ?.getInt("id")
        ?.let(::User)
}

private suspend fun ConnectionPool<*>.acquire(): SuspendingConnection =
    connect().await().asSuspending

private fun createConnectionPool(): ConnectionPool<PostgreSQLConnection> {
    val connectionURL = System.getenv("DATABASE_URL")
    return PostgreSQLConnectionBuilder.createConnectionPool(connectionURL)
}

private fun ApplicationEngine.withGracefulShutdown(
    connectionPool: ConnectionPool<PostgreSQLConnection>
): ApplicationEngine = apply {
    addShutdownHook {
        println("Shutting down DB connection pool...")
        connectionPool.disconnect().get()
        println("DB connection pool successfully shutdown")
    }
}

private fun Application.configureJsonSerialization() {
    install(ContentNegotiation) {
        json(Json {
            prettyPrint = true
            isLenient = true
        })
    }
}