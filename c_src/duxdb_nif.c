#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <erl_nif.h>
#include <duckdb.h>

static ERL_NIF_TERM am_ok;
static ERL_NIF_TERM am_error;
static ERL_NIF_TERM am_true;
static ERL_NIF_TERM am_false;
static ERL_NIF_TERM am_nil;
static ERL_NIF_TERM am_badarg;
static ERL_NIF_TERM am_system_limit;
static ERL_NIF_TERM am___struct__;
static ERL_NIF_TERM am_calendar;
static ERL_NIF_TERM am_year;
static ERL_NIF_TERM am_month;
static ERL_NIF_TERM am_day;
static ERL_NIF_TERM am_hour;
static ERL_NIF_TERM am_minute;
static ERL_NIF_TERM am_second;
static ERL_NIF_TERM am_microsecond;
static ERL_NIF_TERM am_elixir_date;
static ERL_NIF_TERM am_elixir_time;
static ERL_NIF_TERM am_elixir_naive_date_time;
static ERL_NIF_TERM am_elixir_calendar_iso;

static ErlNifResourceType *config_t;
static ErlNifResourceType *db_t;
static ErlNifResourceType *conn_t;
static ErlNifResourceType *result_t;
static ErlNifResourceType *data_chunk_t;
static ErlNifResourceType *stmt_t;

typedef struct
{
    duckdb_config duck;
} duxdb_config;

typedef struct
{
    duckdb_database duck;
} duxdb_db;

typedef struct
{
    duckdb_connection duck;
} duxdb_conn;

typedef struct
{
    duckdb_result duck;
} duxdb_result;

typedef struct
{
    duckdb_data_chunk duck;
} duxdb_data_chunk;

typedef struct
{
    duckdb_prepared_statement duck;
} duxdb_stmt;

static void
config_destructor(ErlNifEnv *env, void *arg)
{
    duckdb_config config = ((duxdb_config *)arg)->duck;
    if (config)
    {
        duckdb_destroy_config(&config);
        assert(config == NULL);
    }
}

static void
db_destructor(ErlNifEnv *env, void *arg)
{
    duckdb_database db = ((duxdb_db *)arg)->duck;
    if (db)
    {
        duckdb_close(&db);
        assert(db == NULL);
    }
}

static void
conn_destructor(ErlNifEnv *env, void *arg)
{
    duckdb_connection conn = ((duxdb_conn *)arg)->duck;
    if (conn)
    {
        duckdb_disconnect(&conn);
        assert(conn == NULL);
    }
}

static void
result_destructor(ErlNifEnv *env, void *arg)
{
    duckdb_result result = ((duxdb_result *)arg)->duck;
    if (result.internal_data)
    {
        duckdb_destroy_result(&result);
        assert(result.internal_data == NULL);
    }
}

static void
data_chunk_destructor(ErlNifEnv *env, void *arg)
{
    duckdb_data_chunk chunk = ((duxdb_data_chunk *)arg)->duck;
    if (chunk)
    {
        duckdb_destroy_data_chunk(&chunk);
        assert(chunk == NULL);
    }
}

static void
stmt_destructor(ErlNifEnv *env, void *arg)
{
    duckdb_prepared_statement stmt = ((duxdb_stmt *)arg)->duck;
    if (stmt)
    {
        duckdb_destroy_prepare(&stmt);
        assert(stmt == NULL);
    }
}

static int
on_load(ErlNifEnv *env, void **priv, ERL_NIF_TERM info)
{
    am_ok = enif_make_atom(env, "ok");
    am_error = enif_make_atom(env, "error");
    am_true = enif_make_atom(env, "true");
    am_false = enif_make_atom(env, "false");
    am_nil = enif_make_atom(env, "nil");
    am_badarg = enif_make_atom(env, "badarg");
    am_system_limit = enif_make_atom(env, "system_limit");
    am___struct__ = enif_make_atom(env, "__struct__");
    am_calendar = enif_make_atom(env, "calendar");
    am_year = enif_make_atom(env, "year");
    am_month = enif_make_atom(env, "month");
    am_day = enif_make_atom(env, "day");
    am_hour = enif_make_atom(env, "hour");
    am_minute = enif_make_atom(env, "minute");
    am_second = enif_make_atom(env, "second");
    am_microsecond = enif_make_atom(env, "microsecond");
    am_elixir_date = enif_make_atom(env, "Elixir.Date");
    am_elixir_time = enif_make_atom(env, "Elixir.Time");
    am_elixir_naive_date_time = enif_make_atom(env, "Elixir.NaiveDateTime");
    am_elixir_calendar_iso = enif_make_atom(env, "Elixir.Calendar.ISO");

    config_t = enif_open_resource_type(env, "duxdb", "config", config_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!config_t)
        return -1;

    db_t = enif_open_resource_type(env, "duxdb", "db", db_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!db_t)
        return -1;

    conn_t = enif_open_resource_type(env, "duxdb", "conn", conn_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!conn_t)
        return -1;

    result_t = enif_open_resource_type(env, "duxdb", "result", result_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!result_t)
        return -1;

    data_chunk_t = enif_open_resource_type(env, "duxdb", "data_chunk", data_chunk_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!data_chunk_t)
        return -1;

    stmt_t = enif_open_resource_type(env, "duxdb", "stmt", stmt_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!stmt_t)
        return -1;

    return 0;
}

static ERL_NIF_TERM
make_binary(ErlNifEnv *env, const char *bytes, size_t size)
{
    ERL_NIF_TERM bin;
    // TODO can fail?
    uint8_t *data = enif_make_new_binary(env, size, &bin);
    memcpy(data, bytes, size);
    return bin;
}

static ERL_NIF_TERM
make_badarg(ErlNifEnv *env, ERL_NIF_TERM arg)
{
    ERL_NIF_TERM badarg = enif_make_tuple2(env, am_badarg, arg);
    return enif_raise_exception(env, badarg);
}

static ERL_NIF_TERM
duxdb_library_version(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    const char *version = duckdb_library_version();
    return make_binary(env, version, strlen(version));
}

static ERL_NIF_TERM
duxdb_create_config(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_config *config = enif_alloc_resource(config_t, sizeof(duxdb_config));
    if (!config)
        return enif_raise_exception(env, am_system_limit);

    if (duckdb_create_config(&config->duck) == DuckDBError)
    {
        enif_release_resource(config);
        duckdb_destroy_config(&config->duck);
        assert(config->duck == NULL);
        return enif_raise_exception(env, am_system_limit);
    }

    ERL_NIF_TERM config_resource = enif_make_resource(env, config);
    enif_release_resource(config);
    return config_resource;
}

static ERL_NIF_TERM
duxdb_config_count(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    size_t count = duckdb_config_count();
    return enif_make_uint64(env, count);
}

static ERL_NIF_TERM
duxdb_get_config_flag(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[0], &idx))
        return make_badarg(env, argv[0]);

    const char *name;
    const char *description;
    if (duckdb_get_config_flag(idx, &name, &description) == DuckDBError)
        return make_badarg(env, argv[0]);

    ERL_NIF_TERM bin_name = make_binary(env, name, strlen(name));
    ERL_NIF_TERM bin_description = make_binary(env, description, strlen(description));
    return enif_make_tuple2(env, bin_name, bin_description);
}

static ERL_NIF_TERM
duxdb_set_config(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_config *config;
    if (!enif_get_resource(env, argv[0], config_t, (void **)&config) || !(config->duck))
        return make_badarg(env, argv[0]);

    ErlNifBinary name;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &name))
        return make_badarg(env, argv[1]);

    ErlNifBinary option;
    if (!enif_inspect_iolist_as_binary(env, argv[2], &option))
        return make_badarg(env, argv[2]);

    if (duckdb_set_config(config->duck, (const char *)name.data, (const char *)option.data) == DuckDBError)
        return am_error;

    return am_ok;
}

static ERL_NIF_TERM
duxdb_destroy_config(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_config *config;
    if (!enif_get_resource(env, argv[0], config_t, (void **)&config))
        return make_badarg(env, argv[0]);

    if (config->duck)
    {
        duckdb_destroy_config(&config->duck);
        assert(config->duck == NULL);
    }

    return am_ok;
}

static ERL_NIF_TERM
duxdb_open_ext(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    const char *duck_path;
    ErlNifBinary path;

    if (enif_inspect_iolist_as_binary(env, argv[0], &path))
        duck_path = (const char *)path.data;
    else if (enif_is_identical(argv[0], am_nil))
        duck_path = NULL;
    else
        return make_badarg(env, argv[0]);

    duckdb_config duck_config;
    duxdb_config *config;

    if (enif_get_resource(env, argv[1], config_t, (void **)&config) && (config->duck))
        duck_config = config->duck;
    else if (enif_is_identical(argv[1], am_nil))
        duck_config = NULL;
    else
        return make_badarg(env, argv[1]);

    duxdb_db *db = enif_alloc_resource(db_t, sizeof(duxdb_db));
    if (!db)
        return enif_raise_exception(env, am_system_limit);

    // sometimes enif_alloc_resource allocates non-zeroed memory
    // which messes up destructors, so we need to zero it out
    db->duck = NULL;

    char *errstr;
    if (duckdb_open_ext(duck_path, &db->duck, duck_config, &errstr) == DuckDBError)
    {
        assert(db->duck == NULL);
        enif_release_resource(db);

        ERL_NIF_TERM error = make_binary(env, errstr, strlen(errstr));
        duckdb_free(errstr);

        return error;
    }

    ERL_NIF_TERM db_resource = enif_make_resource(env, db);
    enif_release_resource(db);
    return db_resource;
}

static ERL_NIF_TERM
duxdb_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_db *db;
    if (!enif_get_resource(env, argv[0], db_t, (void **)&db))
        return make_badarg(env, argv[0]);

    if (db->duck)
    {
        duckdb_close(&db->duck);
        assert(db->duck == NULL);
    }

    return am_ok;
}

static ERL_NIF_TERM
duxdb_connect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_db *db;
    if (!enif_get_resource(env, argv[0], db_t, (void **)&db) || !(db->duck))
        return make_badarg(env, argv[0]);

    duxdb_conn *conn = enif_alloc_resource(conn_t, sizeof(duxdb_conn));
    if (!conn)
        return enif_raise_exception(env, am_system_limit);

    // sometimes enif_alloc_resource allocates non-zeroed memory
    // which messes up destructors, so we need to zero it out
    conn->duck = NULL;

    if (duckdb_connect(db->duck, &conn->duck) == DuckDBError)
    {
        assert(conn->duck == NULL);
        enif_release_resource(conn);
        return enif_raise_exception(env, am_system_limit);
    }

    ERL_NIF_TERM conn_resource = enif_make_resource(env, conn);
    enif_release_resource(conn);
    return conn_resource;
}

static ERL_NIF_TERM
duxdb_interrupt(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_conn *conn;
    if (!enif_get_resource(env, argv[0], conn_t, (void **)&conn) || !(conn->duck))
        return make_badarg(env, argv[0]);

    duckdb_interrupt(conn->duck);
    return am_ok;
}

static ERL_NIF_TERM
duxdb_query_progress(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_conn *conn;
    if (!enif_get_resource(env, argv[0], conn_t, (void **)&conn) || !(conn->duck))
        return make_badarg(env, argv[0]);

    duckdb_query_progress_type progress = duckdb_query_progress(conn->duck);

    ERL_NIF_TERM percentage = enif_make_double(env, progress.percentage);
    ERL_NIF_TERM rows_processed = enif_make_uint64(env, progress.rows_processed);
    ERL_NIF_TERM total_rows_to_process = enif_make_uint64(env, progress.total_rows_to_process);

    return enif_make_tuple3(env, percentage, rows_processed, total_rows_to_process);
}

static ERL_NIF_TERM
duxdb_disconnect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_conn *conn;
    if (!enif_get_resource(env, argv[0], conn_t, (void **)&conn))
        return make_badarg(env, argv[0]);

    if (conn->duck)
    {
        duckdb_disconnect(&conn->duck);
        assert(conn->duck == NULL);
    }

    return am_ok;
}

static ERL_NIF_TERM
duxdb_query(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_conn *conn;
    if (!enif_get_resource(env, argv[0], conn_t, (void **)&conn) || !(conn->duck))
        return make_badarg(env, argv[0]);

    ErlNifBinary query;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &query))
        return make_badarg(env, argv[1]);

    duxdb_result *result = enif_alloc_resource(result_t, sizeof(duxdb_result));
    if (!result)
        return enif_raise_exception(env, am_system_limit);

    if (duckdb_query(conn->duck, (const char *)query.data, &result->duck) == DuckDBError)
    {
        enif_release_resource(result);

        int error_type = (int)duckdb_result_error_type(&result->duck);
        ERL_NIF_TERM errc = enif_make_int(env, error_type);

        const char *error = duckdb_result_error(&result->duck);
        ERL_NIF_TERM errmsg = make_binary(env, error, strlen(error));

        duckdb_destroy_result(&result->duck);
        assert(result->duck.internal_data == NULL);

        return enif_make_tuple2(env, errc, errmsg);
    }

    ERL_NIF_TERM result_resource = enif_make_resource(env, result);
    enif_release_resource(result);
    return result_resource;
}

static ERL_NIF_TERM
duxdb_destroy_result(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_result *result;
    if (!enif_get_resource(env, argv[0], result_t, (void **)&result))
        return make_badarg(env, argv[0]);

    if (result->duck.internal_data)
    {
        duckdb_destroy_result(&result->duck);
        assert(result->duck.internal_data == NULL);
    }

    return am_ok;
}

static ERL_NIF_TERM
duxdb_column_name(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_result *result;
    if (!enif_get_resource(env, argv[0], result_t, (void **)&result) || !(result->duck.internal_data))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    const char *name = duckdb_column_name(&result->duck, idx);
    if (!name)
        return am_nil;

    return make_binary(env, name, strlen(name));
}

static ERL_NIF_TERM
duxdb_result_statement_type(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_result *result;
    if (!enif_get_resource(env, argv[0], result_t, (void **)&result) || !(result->duck.internal_data))
        return make_badarg(env, argv[0]);

    duckdb_statement_type type = duckdb_result_statement_type(result->duck);
    return enif_make_int(env, type);
}

static ERL_NIF_TERM
duxdb_column_count(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_result *result;
    if (!enif_get_resource(env, argv[0], result_t, (void **)&result) || !(result->duck.internal_data))
        return make_badarg(env, argv[0]);

    idx_t count = duckdb_column_count(&result->duck);
    return enif_make_uint64(env, count);
}

static ERL_NIF_TERM
duxdb_rows_changed(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_result *result;
    if (!enif_get_resource(env, argv[0], result_t, (void **)&result) || !(result->duck.internal_data))
        return make_badarg(env, argv[0]);

    idx_t count = duckdb_rows_changed(&result->duck);
    return enif_make_uint64(env, count);
}

static ERL_NIF_TERM
duxdb_result_return_type(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_result *result;
    if (!enif_get_resource(env, argv[0], result_t, (void **)&result) || !(result->duck.internal_data))
        return make_badarg(env, argv[0]);

    duckdb_result_type result_type = duckdb_result_return_type(result->duck);
    return enif_make_int(env, result_type);
}

static ERL_NIF_TERM
duxdb_fetch_chunk(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_result *result;
    if (!enif_get_resource(env, argv[0], result_t, (void **)&result) || !(result->duck.internal_data))
        return make_badarg(env, argv[0]);

    duxdb_data_chunk *chunk = enif_alloc_resource(data_chunk_t, sizeof(duxdb_data_chunk));
    if (!chunk)
        return enif_raise_exception(env, am_system_limit);

    chunk->duck = duckdb_fetch_chunk(result->duck);
    if (!chunk->duck)
    {
        enif_release_resource(chunk);
        return am_nil;
    }

    ERL_NIF_TERM chunk_resource = enif_make_resource(env, chunk);
    enif_release_resource(chunk);
    return chunk_resource;
}

static ERL_NIF_TERM
duxdb_destroy_data_chunk(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_data_chunk *chunk;
    if (!enif_get_resource(env, argv[0], data_chunk_t, (void **)&chunk))
        return make_badarg(env, argv[0]);

    if (chunk->duck)
    {
        duckdb_destroy_data_chunk(&chunk->duck);
        assert(chunk->duck == NULL);
    }

    return am_ok;
}

static ERL_NIF_TERM
duxdb_data_chunk_get_column_count(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_data_chunk *chunk;
    if (!enif_get_resource(env, argv[0], data_chunk_t, (void **)&chunk) || !(chunk->duck))
        return make_badarg(env, argv[0]);

    idx_t count = duckdb_data_chunk_get_column_count(chunk->duck);
    return enif_make_uint64(env, count);
}

static inline ERL_NIF_TERM
make_bool(ErlNifEnv *env, bool b)
{
    return b ? am_true : am_false;
}

// TODO finite / infinite
static inline ERL_NIF_TERM
make_date(ErlNifEnv *env, const duckdb_date duck_date)
{
    duckdb_date_struct d = duckdb_from_date(duck_date);

    ERL_NIF_TERM erl_date;
    // TODO handle error
    enif_make_map_from_arrays(
        env,
        (ERL_NIF_TERM[]){
            am___struct__,
            am_calendar,
            am_year,
            am_month,
            am_day},
        (ERL_NIF_TERM[]){
            am_elixir_date,
            am_elixir_calendar_iso,
            enif_make_int(env, d.year),
            enif_make_int(env, d.month),
            enif_make_int(env, d.day)},
        5,
        &erl_date);

    return erl_date;
}

// // TODO
// int micros_precision(int32_t microseconds)
// {
//     if (microseconds == 0)
//         return 0;
//     if (microseconds % 1000 == 0)
//         return 3;
//     return 6;
// }

static inline ERL_NIF_TERM
make_time(ErlNifEnv *env, const duckdb_time duck_time)
{
    duckdb_time_struct t = duckdb_from_time(duck_time);

    ERL_NIF_TERM erl_time;
    // TODO handle error
    enif_make_map_from_arrays(
        env,
        (ERL_NIF_TERM[]){
            am___struct__,
            am_calendar,
            am_hour,
            am_minute,
            am_second,
            am_microsecond},
        (ERL_NIF_TERM[]){
            am_elixir_time,
            am_elixir_calendar_iso,
            enif_make_int(env, t.hour),
            enif_make_int(env, t.min),
            enif_make_int(env, t.sec),
            enif_make_tuple2(env, enif_make_int(env, t.micros), enif_make_int(env, 6))},
        6,
        &erl_time);

    return erl_time;
}

// TODO finite / infinite
static inline ERL_NIF_TERM
make_naive_datetime(ErlNifEnv *env, const duckdb_timestamp duck_timestamp)
{
    duckdb_timestamp_struct ts = duckdb_from_timestamp(duck_timestamp);
    duckdb_date_struct d = ts.date;
    duckdb_time_struct t = ts.time;

    ERL_NIF_TERM naive_datetime;
    // TODO handle error
    enif_make_map_from_arrays(
        env,
        (ERL_NIF_TERM[]){
            am___struct__,
            am_calendar,
            am_year,
            am_month,
            am_day,
            am_hour,
            am_minute,
            am_second,
            am_microsecond},
        (ERL_NIF_TERM[]){
            am_elixir_naive_date_time,
            am_elixir_calendar_iso,
            enif_make_int(env, d.year),
            enif_make_int(env, d.month),
            enif_make_int(env, d.day),
            enif_make_int(env, t.hour),
            enif_make_int(env, t.min),
            enif_make_int(env, t.sec),
            enif_make_tuple2(env, enif_make_int(env, t.micros), enif_make_int(env, 6))},
        9,
        &naive_datetime);

    return naive_datetime;
}

static inline ERL_NIF_TERM
make_hugeint(ErlNifEnv *env, duckdb_hugeint hi)
{
    return enif_make_tuple2(env, enif_make_int64(env, hi.upper), enif_make_uint64(env, hi.lower));
}

static inline ERL_NIF_TERM
make_uhugeint(ErlNifEnv *env, duckdb_uhugeint uhi)
{
    return enif_make_tuple2(env, enif_make_uint64(env, uhi.upper), enif_make_uint64(env, uhi.lower));
}

static inline ERL_NIF_TERM
make_binary_from_duckdb_string(ErlNifEnv *env, duckdb_string_t str)
{
    return make_binary(env, duckdb_string_t_data(&str), duckdb_string_t_length(str));
}

#define MAKE_LIST_FROM_VECTOR(ctype, term_maker)                                                                       \
    static inline ERL_NIF_TERM make_list_from_##ctype##_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector) \
    {                                                                                                                  \
        ERL_NIF_TERM terms[chunk_size];                                                                                \
        ctype *data = (ctype *)duckdb_vector_get_data(vector);                                                         \
        uint64_t *validity = duckdb_vector_get_validity(vector);                                                       \
        if (validity)                                                                                                  \
        {                                                                                                              \
            for (idx_t i = 0; i < chunk_size; i++)                                                                     \
                terms[i] = duckdb_validity_row_is_valid(validity, i) ? term_maker(env, data[i]) : am_nil;              \
        }                                                                                                              \
        else                                                                                                           \
        {                                                                                                              \
            for (idx_t i = 0; i < chunk_size; i++)                                                                     \
                terms[i] = term_maker(env, data[i]);                                                                   \
        }                                                                                                              \
        return enif_make_list_from_array(env, terms, chunk_size);                                                      \
    }

MAKE_LIST_FROM_VECTOR(bool, make_bool)
MAKE_LIST_FROM_VECTOR(int8_t, enif_make_int)
MAKE_LIST_FROM_VECTOR(int16_t, enif_make_int)
MAKE_LIST_FROM_VECTOR(int32_t, enif_make_int)
MAKE_LIST_FROM_VECTOR(int64_t, enif_make_int64)
MAKE_LIST_FROM_VECTOR(uint8_t, enif_make_uint)
MAKE_LIST_FROM_VECTOR(uint16_t, enif_make_uint)
MAKE_LIST_FROM_VECTOR(uint32_t, enif_make_uint)
MAKE_LIST_FROM_VECTOR(uint64_t, enif_make_uint64)
MAKE_LIST_FROM_VECTOR(float, enif_make_double)
MAKE_LIST_FROM_VECTOR(double, enif_make_double)
MAKE_LIST_FROM_VECTOR(duckdb_date, make_date)
MAKE_LIST_FROM_VECTOR(duckdb_time, make_time)
MAKE_LIST_FROM_VECTOR(duckdb_timestamp, make_naive_datetime)
MAKE_LIST_FROM_VECTOR(duckdb_string_t, make_binary_from_duckdb_string)
MAKE_LIST_FROM_VECTOR(duckdb_hugeint, make_hugeint)
MAKE_LIST_FROM_VECTOR(duckdb_uhugeint, make_uhugeint)

// TODO nested lists
#define MAKE_LIST_FROM_LIST_VECTOR(ctype, term_maker)                                                                           \
    static inline ERL_NIF_TERM make_list_from_##ctype##_list_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)     \
    {                                                                                                                           \
        ERL_NIF_TERM terms[chunk_size];                                                                                         \
        duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(vector);                                       \
        uint64_t *entries_validity = duckdb_vector_get_validity(vector);                                                        \
        duckdb_vector child = duckdb_list_vector_get_child(vector);                                                             \
        ctype *data = (ctype *)duckdb_vector_get_data(child);                                                                   \
        uint64_t *validity = duckdb_vector_get_validity(child);                                                                 \
        for (idx_t i = 0; i < chunk_size; i++)                                                                                  \
        {                                                                                                                       \
            if (duckdb_validity_row_is_valid(entries_validity, i))                                                              \
            {                                                                                                                   \
                duckdb_list_entry entry = entries[i];                                                                           \
                ERL_NIF_TERM subterms[entry.length];                                                                            \
                for (idx_t j = entry.offset; j < entry.offset + entry.length; j++)                                              \
                    subterms[j - entry.offset] = duckdb_validity_row_is_valid(validity, j) ? term_maker(env, data[j]) : am_nil; \
                terms[i] = enif_make_list_from_array(env, subterms, entry.length);                                              \
            }                                                                                                                   \
            else                                                                                                                \
            {                                                                                                                   \
                terms[i] = am_nil;                                                                                              \
            }                                                                                                                   \
        }                                                                                                                       \
        return enif_make_list_from_array(env, terms, chunk_size);                                                               \
    }

MAKE_LIST_FROM_LIST_VECTOR(bool, make_bool)
MAKE_LIST_FROM_LIST_VECTOR(int8_t, enif_make_int)
MAKE_LIST_FROM_LIST_VECTOR(int16_t, enif_make_int)
MAKE_LIST_FROM_LIST_VECTOR(int32_t, enif_make_int)
MAKE_LIST_FROM_LIST_VECTOR(int64_t, enif_make_int64)
MAKE_LIST_FROM_LIST_VECTOR(uint8_t, enif_make_uint)
MAKE_LIST_FROM_LIST_VECTOR(uint16_t, enif_make_uint)
MAKE_LIST_FROM_LIST_VECTOR(uint32_t, enif_make_uint)
MAKE_LIST_FROM_LIST_VECTOR(uint64_t, enif_make_uint64)
MAKE_LIST_FROM_LIST_VECTOR(float, enif_make_double)
MAKE_LIST_FROM_LIST_VECTOR(double, enif_make_double)
MAKE_LIST_FROM_LIST_VECTOR(duckdb_date, make_date)
MAKE_LIST_FROM_LIST_VECTOR(duckdb_time, make_time)
MAKE_LIST_FROM_LIST_VECTOR(duckdb_timestamp, make_naive_datetime)
MAKE_LIST_FROM_LIST_VECTOR(duckdb_string_t, make_binary_from_duckdb_string)
MAKE_LIST_FROM_LIST_VECTOR(duckdb_hugeint, make_hugeint)
MAKE_LIST_FROM_LIST_VECTOR(duckdb_uhugeint, make_uhugeint)

static ERL_NIF_TERM
duxdb_data_chunk_get_vector(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_data_chunk *chunk;
    if (!enif_get_resource(env, argv[0], data_chunk_t, (void **)&chunk) || !(chunk->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    idx_t chunk_size = duckdb_data_chunk_get_size(chunk->duck);

    duckdb_vector vector = duckdb_data_chunk_get_vector(chunk->duck, idx);
    if (!vector)
        return make_badarg(env, argv[1]);

    duckdb_logical_type logical_type = duckdb_vector_get_column_type(vector);
    duckdb_type type = duckdb_get_type_id(logical_type);
    duckdb_destroy_logical_type(&logical_type);

    switch (type)
    {
    case DUCKDB_TYPE_BOOLEAN:
        return make_list_from_bool_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_INTEGER:
        return make_list_from_int32_t_vector(env, chunk_size, vector);
    case DUCKDB_TYPE_UINTEGER:
        return make_list_from_uint32_t_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_VARCHAR:
    case DUCKDB_TYPE_BLOB:
        return make_list_from_duckdb_string_t_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_BIGINT:
        return make_list_from_int64_t_vector(env, chunk_size, vector);
    case DUCKDB_TYPE_UBIGINT:
        return make_list_from_uint64_t_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_FLOAT:
        return make_list_from_float_vector(env, chunk_size, vector);
    case DUCKDB_TYPE_DOUBLE:
        return make_list_from_double_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_TIMESTAMP:
    case DUCKDB_TYPE_TIMESTAMP_S:
    case DUCKDB_TYPE_TIMESTAMP_MS:
    case DUCKDB_TYPE_TIMESTAMP_NS:
        return make_list_from_duckdb_timestamp_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_DATE:
        return make_list_from_duckdb_date_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_TIME:
        return make_list_from_duckdb_time_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_TINYINT:
        return make_list_from_int8_t_vector(env, chunk_size, vector);
    case DUCKDB_TYPE_UTINYINT:
        return make_list_from_uint8_t_vector(env, chunk_size, vector);
    case DUCKDB_TYPE_SMALLINT:
        return make_list_from_int16_t_vector(env, chunk_size, vector);
    case DUCKDB_TYPE_USMALLINT:
        return make_list_from_uint16_t_vector(env, chunk_size, vector);

    // TODO
    case DUCKDB_TYPE_HUGEINT:
    {
        ERL_NIF_TERM i128s = make_list_from_duckdb_hugeint_vector(env, chunk_size, vector);
        return enif_make_tuple2(env, enif_make_int(env, DUCKDB_TYPE_HUGEINT), i128s);
    }
    case DUCKDB_TYPE_UHUGEINT:
    {
        ERL_NIF_TERM u128s = make_list_from_duckdb_uhugeint_vector(env, chunk_size, vector);
        return enif_make_tuple2(env, enif_make_int(env, DUCKDB_TYPE_UHUGEINT), u128s);
    }

    case DUCKDB_TYPE_LIST:
    {
        duckdb_logical_type logical_type = duckdb_vector_get_column_type(vector);
        duckdb_logical_type child_logical_type = duckdb_list_type_child_type(logical_type);
        duckdb_destroy_logical_type(&logical_type);
        duckdb_type child_type = duckdb_get_type_id(child_logical_type);
        duckdb_destroy_logical_type(&child_logical_type);

        switch (child_type)
        {
        case DUCKDB_TYPE_BOOLEAN:
            return make_list_from_bool_list_vector(env, chunk_size, vector);

        case DUCKDB_TYPE_INTEGER:
            return make_list_from_int32_t_list_vector(env, chunk_size, vector);
        case DUCKDB_TYPE_UINTEGER:
            return make_list_from_uint32_t_list_vector(env, chunk_size, vector);

        case DUCKDB_TYPE_VARCHAR:
        case DUCKDB_TYPE_BLOB:
            return make_list_from_duckdb_string_t_list_vector(env, chunk_size, vector);

        case DUCKDB_TYPE_BIGINT:
            return make_list_from_int64_t_list_vector(env, chunk_size, vector);
        case DUCKDB_TYPE_UBIGINT:
            return make_list_from_uint64_t_list_vector(env, chunk_size, vector);

        case DUCKDB_TYPE_FLOAT:
            return make_list_from_float_list_vector(env, chunk_size, vector);
        case DUCKDB_TYPE_DOUBLE:
            return make_list_from_double_list_vector(env, chunk_size, vector);

        case DUCKDB_TYPE_TIMESTAMP:
        case DUCKDB_TYPE_TIMESTAMP_S:
        case DUCKDB_TYPE_TIMESTAMP_MS:
        case DUCKDB_TYPE_TIMESTAMP_NS:
            return make_list_from_duckdb_timestamp_list_vector(env, chunk_size, vector);

        case DUCKDB_TYPE_DATE:
            return make_list_from_duckdb_date_list_vector(env, chunk_size, vector);

        case DUCKDB_TYPE_TIME:
            return make_list_from_duckdb_time_list_vector(env, chunk_size, vector);

        case DUCKDB_TYPE_TINYINT:
            return make_list_from_int8_t_list_vector(env, chunk_size, vector);
        case DUCKDB_TYPE_UTINYINT:
            return make_list_from_uint8_t_list_vector(env, chunk_size, vector);
        case DUCKDB_TYPE_SMALLINT:
            return make_list_from_int16_t_list_vector(env, chunk_size, vector);
        case DUCKDB_TYPE_USMALLINT:
            return make_list_from_uint16_t_list_vector(env, chunk_size, vector);

        // TODO
        case DUCKDB_TYPE_HUGEINT:
        {
            ERL_NIF_TERM i128s = make_list_from_duckdb_hugeint_list_vector(env, chunk_size, vector);
            return enif_make_tuple2(env, enif_make_int(env, DUCKDB_TYPE_HUGEINT), i128s);
        }
        case DUCKDB_TYPE_UHUGEINT:
        {
            ERL_NIF_TERM u128s = make_list_from_duckdb_uhugeint_list_vector(env, chunk_size, vector);
            return enif_make_tuple2(env, enif_make_int(env, DUCKDB_TYPE_UHUGEINT), u128s);
        }

        default:
            // TODO
            return make_badarg(env, enif_make_tuple2(env, enif_make_atom(env, "DUCKDB_TYPE"), enif_make_int(env, type)));
        }
    }

    default:
        // TODO
        return make_badarg(env, enif_make_tuple2(env, enif_make_atom(env, "DUCKDB_TYPE"), enif_make_int(env, type)));
    }
}

static ERL_NIF_TERM
duxdb_prepare_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_conn *conn;
    if (!enif_get_resource(env, argv[0], conn_t, (void **)&conn) || !(conn->duck))
        return make_badarg(env, argv[0]);

    ErlNifBinary query;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &query))
        return make_badarg(env, argv[1]);

    duxdb_stmt *stmt = enif_alloc_resource(stmt_t, sizeof(duxdb_stmt));
    if (!stmt)
        return enif_raise_exception(env, am_system_limit);

    if (duckdb_prepare(conn->duck, (const char *)query.data, &stmt->duck) == DuckDBError)
    {
        enif_release_resource(stmt);
        const char *error = duckdb_prepare_error(stmt->duck);
        return make_binary(env, error, strlen(error));
    }

    ERL_NIF_TERM stmt_resource = enif_make_resource(env, stmt);
    enif_release_resource(stmt);
    return stmt_resource;
}

static ERL_NIF_TERM
duxdb_destroy_prepare(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt))
        return make_badarg(env, argv[0]);

    if (stmt->duck)
    {
        duckdb_destroy_prepare(&stmt->duck);
        assert(stmt->duck == NULL);
    }

    return am_ok;
}

static ERL_NIF_TERM
duxdb_nparams(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    idx_t count = duckdb_nparams(stmt->duck);
    return enif_make_uint64(env, count);
}

static ERL_NIF_TERM
duxdb_parameter_name(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    const char *parameter_name = duckdb_parameter_name(stmt->duck, idx);
    if (!parameter_name)
        return make_badarg(env, argv[1]);

    ERL_NIF_TERM name = make_binary(env, parameter_name, strlen(parameter_name));
    // TODO
    duckdb_free((char *)parameter_name);
    return name;
}

static ERL_NIF_TERM
duxdb_param_type(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    duckdb_type param_type = duckdb_param_type(stmt->duck, idx);
    if (param_type == DUCKDB_TYPE_INVALID)
        return make_badarg(env, argv[1]);

    return enif_make_int(env, param_type);
}

static ERL_NIF_TERM
duxdb_bind_parameter_index(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifBinary name;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &name))
        return make_badarg(env, argv[1]);

    idx_t idx;
    if (duckdb_bind_parameter_index(stmt->duck, &idx, (const char *)name.data) == DuckDBError)
        return make_badarg(env, argv[1]);

    return enif_make_uint64(env, idx);
}

static ERL_NIF_TERM
duxdb_clear_bindings(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    if (duckdb_clear_bindings(stmt->duck) == DuckDBError)
        return make_badarg(env, argv[0]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_prepared_statement_type(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    duckdb_statement_type type = duckdb_prepared_statement_type(stmt->duck);
    return enif_make_int(env, type);
}

static ERL_NIF_TERM
duxdb_execute_prepared(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    duxdb_result *result = enif_alloc_resource(result_t, sizeof(duxdb_result));
    if (!result)
        return enif_raise_exception(env, am_system_limit);

    if (duckdb_execute_prepared(stmt->duck, &result->duck) == DuckDBError)
    {
        enif_release_resource(result);

        int error_type = (int)duckdb_result_error_type(&result->duck);
        ERL_NIF_TERM errc = enif_make_int(env, error_type);

        const char *error = duckdb_result_error(&result->duck);
        ERL_NIF_TERM errmsg = make_binary(env, error, strlen(error));

        duckdb_destroy_result(&result->duck);
        assert(result->duck.internal_data == NULL);

        return enif_make_tuple2(env, errc, errmsg);
    }

    ERL_NIF_TERM result_resource = enif_make_resource(env, result);
    enif_release_resource(result);
    return result_resource;
}

static ERL_NIF_TERM
duxdb_bind_boolean(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    // TODO refactor
    duckdb_state rc;
    if (enif_is_identical(argv[2], am_true))
    {
        rc = duckdb_bind_boolean(stmt->duck, idx, 1);
    }
    else if (enif_is_identical(argv[2], am_false))
    {
        rc = duckdb_bind_boolean(stmt->duck, idx, 0);
    }
    else
    {
        return make_badarg(env, argv[2]);
    }

    if (rc == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_double(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    double f64;
    if (!enif_get_double(env, argv[2], &f64) || duckdb_bind_double(stmt->duck, idx, f64) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_int64(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifSInt64 i64;
    if (!enif_get_int64(env, argv[2], &i64) || duckdb_bind_int64(stmt->duck, idx, i64) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_uint64(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifUInt64 u64;
    if (!enif_get_uint64(env, argv[2], &u64) || duckdb_bind_uint64(stmt->duck, idx, u64) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_varchar(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifBinary value;
    if (!enif_inspect_binary(env, argv[2], &value) || duckdb_bind_varchar_length(stmt->duck, idx, (const char *)value.data, value.size) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_blob(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifBinary value;
    if (!enif_inspect_binary(env, argv[2], &value) || duckdb_bind_blob(stmt->duck, idx, (const char *)value.data, value.size) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_date(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifSInt64 days;
    if (!enif_get_int64(env, argv[2], &days) || duckdb_bind_date(stmt->duck, idx, (duckdb_date){.days = days}) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_time(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifSInt64 micros;
    if (!enif_get_int64(env, argv[2], &micros) || duckdb_bind_time(stmt->duck, idx, (duckdb_time){.micros = micros}) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_timestamp(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifSInt64 micros;
    if (!enif_get_int64(env, argv[2], &micros) || duckdb_bind_timestamp(stmt->duck, idx, (duckdb_timestamp){.micros = micros}) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_hugeint(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifSInt64 upper;
    ErlNifUInt64 lower;

    if (!enif_get_int64(env, argv[2], &upper) ||
        !enif_get_uint64(env, argv[3], &lower) ||
        duckdb_bind_hugeint(stmt->duck, idx, (duckdb_hugeint){.lower = lower, .upper = upper}) == DuckDBError)
        return am_error;

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_uhugeint(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifUInt64 upper;
    ErlNifUInt64 lower;

    if (!enif_get_uint64(env, argv[2], &upper) ||
        !enif_get_uint64(env, argv[3], &lower) ||
        duckdb_bind_uhugeint(stmt->duck, idx, (duckdb_uhugeint){.lower = lower, .upper = upper}) == DuckDBError)
        return am_error;

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_null(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx) || duckdb_bind_null(stmt->duck, idx) == DuckDBError)
        return make_badarg(env, argv[1]);

    return am_ok;
}

static ErlNifFunc nif_funcs[] = {
    {"library_version", 0, duxdb_library_version, 0},

    {"config_count", 0, duxdb_config_count, 0},
    {"get_config_flag", 1, duxdb_get_config_flag, 0},

    // The create_config function is typically called first and can take a
    // significant amount of time on the initial call, likely due to the library
    // initialization or resource loading it triggers. For this reason, it is
    // scheduled as a CPU-bound dirty job to avoid blocking the main Erlang scheduler.
    {"create_config", 0, duxdb_create_config, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"set_config", 3, duxdb_set_config, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"destroy_config", 1, duxdb_destroy_config, ERL_NIF_DIRTY_JOB_CPU_BOUND},

    {"open_ext", 2, duxdb_open_ext, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"close", 1, duxdb_close, ERL_NIF_DIRTY_JOB_IO_BOUND},

    {"connect", 1, duxdb_connect, 0},
    {"interrupt", 1, duxdb_interrupt, 0},
    {"query_progress", 1, duxdb_query_progress, 0},
    {"disconnect", 1, duxdb_disconnect, 0},

    // TODO remove, leave dirty only
    {"query_nif", 2, duxdb_query, 0},
    {"query_dirty_io_nif", 2, duxdb_query, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"query_dirty_cpu_nif", 2, duxdb_query, ERL_NIF_DIRTY_JOB_CPU_BOUND},

    {"destroy_result", 1, duxdb_destroy_result, 0},
    {"column_name", 2, duxdb_column_name, 0},
    {"result_statement_type", 1, duxdb_result_statement_type, 0},
    {"column_count", 1, duxdb_column_count, 0},
    {"rows_changed", 1, duxdb_rows_changed, 0},
    {"result_return_type", 1, duxdb_result_return_type, 0},

    {"fetch_chunk", 1, duxdb_fetch_chunk, 0},
    // TODO destoy once empty
    {"destroy_data_chunk", 1, duxdb_destroy_data_chunk, 0},
    {"data_chunk_get_column_count", 1, duxdb_data_chunk_get_column_count, 0},
    {"data_chunk_get_vector_nif", 2, duxdb_data_chunk_get_vector, 0},

    {"prepare_nif", 2, duxdb_prepare_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},

    {"destroy_prepare", 1, duxdb_destroy_prepare, 0},
    {"nparams", 1, duxdb_nparams, 0},
    {"parameter_name", 2, duxdb_parameter_name, 0},
    {"param_type", 2, duxdb_param_type, 0},
    {"bind_parameter_index_nif", 2, duxdb_bind_parameter_index, 0},
    {"clear_bindings", 1, duxdb_clear_bindings, 0},
    {"prepared_statement_type", 1, duxdb_prepared_statement_type, 0},

    // TODO remove, leave dirty only
    {"execute_prepared_nif", 1, duxdb_execute_prepared, 0},
    {"execute_prepared_dirty_io_nif", 1, duxdb_execute_prepared, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"execute_prepared_dirty_cpu_nif", 1, duxdb_execute_prepared, ERL_NIF_DIRTY_JOB_CPU_BOUND},

    {"bind_boolean", 3, duxdb_bind_boolean, 0},
    {"bind_double", 3, duxdb_bind_double, 0},
    {"bind_int64", 3, duxdb_bind_int64, 0},
    {"bind_uint64", 3, duxdb_bind_uint64, 0},
    {"bind_varchar", 3, duxdb_bind_varchar, 0},
    {"bind_blob", 3, duxdb_bind_blob, 0},
    {"bind_date_nif", 3, duxdb_bind_date, 0},
    {"bind_time_nif", 3, duxdb_bind_time, 0},
    {"bind_timestamp_nif", 3, duxdb_bind_timestamp, 0},
    {"bind_hugeint_nif", 4, duxdb_bind_hugeint, 0},
    {"bind_uhugeint_nif", 4, duxdb_bind_uhugeint, 0},
    {"bind_null", 2, duxdb_bind_null, 0},
};

ERL_NIF_INIT(Elixir.DuxDB, nif_funcs, on_load, NULL, NULL, NULL)
