#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

// Elixir workaround for . in module names
#ifdef STATIC_ERLANG_NIF
#define STATIC_ERLANG_NIF_LIBNAME duxdb_nif
#endif

#include <erl_nif.h>
#include <duckdb.h>

#define MAX_ATOM_LENGTH 255

static ERL_NIF_TERM am_ok;
static ERL_NIF_TERM am_error;
static ERL_NIF_TERM am_nil;
static ERL_NIF_TERM am_out_of_memory;
static ERL_NIF_TERM am_done;
static ERL_NIF_TERM am_row;
static ERL_NIF_TERM am_rows;

static ErlNifResourceType *db_type = NULL;
static ErlNifResourceType *conn_type = NULL;
static ErlNifResourceType *stmt_type = NULL;

typedef struct db
{
    duckdb_database db;
} db_t;

typedef struct conn
{
    duckdb_connection *conn;
} conn_t;

typedef struct stmt
{
    duckdb_prepared_statement *stmt;
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

    char path[256];
    if (enif_get_string(env, argv[0], path, sizeof(path), ERL_NIF_UTF8) < 0)
        return enif_make_badarg(env);

    duckdb_database duckdb_database;
    duckdb_state state = duckdb_open(path, &duckdb_database);

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
        duckdb_disconnect(conn->conn);
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
        duckdb_destroy_prepare(stmt->stmt);
        stmt->stmt = NULL;
    }
}

static int
on_load(ErlNifEnv *env, void **priv, ERL_NIF_TERM info)
{
    assert(env);

    // TODO
    am_ok = enif_make_atom(env, "ok");
    am_error = enif_make_atom(env, "error");
    am_nil = enif_make_atom(env, "nil");
    // TODO rename to alloc_error
    am_out_of_memory = enif_make_atom(env, "out_of_memory");
    am_done = enif_make_atom(env, "done");
    am_row = enif_make_atom(env, "row");
    am_rows = enif_make_atom(env, "rows");

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
};

ERL_NIF_INIT(Elixir.DuxDB, nif_funcs, on_load, NULL, NULL, NULL)
