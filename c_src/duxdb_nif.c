#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <erl_nif.h>
#include <duckdb.h>

static ERL_NIF_TERM am_ok;
static ERL_NIF_TERM am_error;
static ERL_NIF_TERM am_nil;
static ERL_NIF_TERM am_out_of_memory;
static ErlNifResourceType *db_type = NULL;
static ErlNifResourceType *conn_type = NULL;
static ErlNifResourceType *stmt_type = NULL;

typedef struct db
{
    duckdb_database db;
} db_t;

typedef struct conn
{
    duckdb_connection conn;
} conn_t;

typedef struct stmt
{
    duckdb_prepared_statement stmt;
} stmt_t;

static ERL_NIF_TERM
make_binary(ErlNifEnv *env, const char *bytes, size_t size)
{
    ERL_NIF_TERM bin;
    uint8_t *data = enif_make_new_binary(env, size, &bin);
    memcpy(data, bytes, size);
    return bin;
}

static ERL_NIF_TERM
duxdb_library_version(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    assert(argc == 0);
    const char *version = duckdb_library_version();
    return make_binary(env, version, strlen(version));
}

static ERL_NIF_TERM
duxdb_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    assert(argc == 1);

    ErlNifBinary path;
    if (!enif_inspect_binary(env, argv[0], &path))
        return enif_make_badarg(env);

    duckdb_database duckdb_database;
    duckdb_state state = duckdb_open((char *)path.data, &duckdb_database);

    if (state == DuckDBError)
        return enif_raise_exception(env, am_error);

    db_t *db;
    db = enif_alloc_resource(db_type, sizeof(db_t));

    if (!db)
    {
        duckdb_close(&duckdb_database);
        return enif_raise_exception(env, am_out_of_memory);
    }

    db->db = duckdb_database;
    ERL_NIF_TERM result = enif_make_resource(env, db);
    enif_release_resource(db);
    return result;
}

static ERL_NIF_TERM
duxdb_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    assert(argc == 1);

    db_t *db;
    if (!enif_get_resource(env, argv[0], db_type, (void **)&db))
        return enif_make_badarg(env);

    if (db->db == NULL)
        return am_ok;

    duckdb_close(&db->db);
    db->db = NULL;
    return am_ok;
}

static ERL_NIF_TERM
duxdb_connect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    assert(argc == 1);

    db_t *db;
    if (!enif_get_resource(env, argv[0], db_type, (void **)&db))
        return enif_make_badarg(env);

    duckdb_connection duckdb_connection;
    duckdb_state state = duckdb_connect(db->db, &duckdb_connection);

    if (state == DuckDBError)
        return enif_raise_exception(env, am_error);

    conn_t *conn;
    conn = enif_alloc_resource(conn_type, sizeof(conn_t));

    if (!conn)
    {
        duckdb_disconnect(&duckdb_connection);
        return enif_raise_exception(env, am_out_of_memory);
    }

    conn->conn = duckdb_connection;
    ERL_NIF_TERM result = enif_make_resource(env, conn);
    enif_release_resource(conn);
    return result;
}

static ERL_NIF_TERM
duxdb_disconnect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    assert(argc == 1);

    conn_t *conn;
    if (!enif_get_resource(env, argv[0], conn_type, (void **)&conn))
        return enif_make_badarg(env);

    if (conn->conn == NULL)
        return am_ok;

    duckdb_disconnect(&conn->conn);
    conn->conn = NULL;
    return am_ok;
}

static void
db_type_destructor(ErlNifEnv *env, void *arg)
{
    assert(env);
    assert(arg);

    db_t *db = (db_t *)arg;

    if (db->db)
    {
        duckdb_close(&db->db);
        db->db = NULL;
    }
}

static void
conn_type_destructor(ErlNifEnv *env, void *arg)
{

    assert(env);
    assert(arg);

    conn_t *conn = (conn_t *)arg;

    if (conn->conn)
    {
        duckdb_disconnect(&conn->conn);
        conn->conn = NULL;
    }
}

static void
stmt_type_destructor(ErlNifEnv *env, void *arg)
{
    assert(env);
    assert(arg);

    stmt_t *stmt = (stmt_t *)arg;

    if (stmt->stmt)
    {
        duckdb_destroy_prepare(&stmt->stmt);
        stmt->stmt = NULL;
    }
}

static int
on_load(ErlNifEnv *env, void **priv, ERL_NIF_TERM info)
{
    assert(env);

    am_ok = enif_make_atom(env, "ok");
    am_error = enif_make_atom(env, "error");
    am_nil = enif_make_atom(env, "nil");
    am_out_of_memory = enif_make_atom(env, "out_of_memory");

    db_type = enif_open_resource_type(env, "duxdb", "db_type", db_type_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!db_type)
        return -1;

    conn_type = enif_open_resource_type(env, "duxdb", "conn_type", conn_type_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!conn_type)
        return -1;

    stmt_type = enif_open_resource_type(env, "duxdb", "stmt_type", stmt_type_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!stmt_type)
        return -1;

    return 0;
}

static ErlNifFunc nif_funcs[] = {
    {"library_version", 0, duxdb_library_version},
    {"dirty_io_open_nif", 1, duxdb_open, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"dirty_io_close_nif", 1, duxdb_close, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"connect", 1, duxdb_connect},
    {"disconnect", 1, duxdb_disconnect},
};

ERL_NIF_INIT(Elixir.DuxDB, nif_funcs, on_load, NULL, NULL, NULL)
