from pychord.db import get_value_by_key, does_key_exist, set_key_value_pair, remove_key, get_all_kv_pairs, \
    transaction_wrapper


def test_db_crud(database_conn):
    assert not get_all_kv_pairs(database_conn)
    assert not does_key_exist(database_conn, "foo")
    assert not get_value_by_key(database_conn, "foo")

    with transaction_wrapper(database_conn) as t:
        set_key_value_pair(t, "foo", "bar")
        set_key_value_pair(t, "baz", {"hello": "world"})

    assert does_key_exist(database_conn, "foo")
    assert does_key_exist(database_conn, "baz")
    assert not does_key_exist(database_conn, "wtf")

    assert get_value_by_key(database_conn, "foo") == "bar"
    assert get_value_by_key(database_conn, "baz") == {"hello": "world"}
    assert get_value_by_key(database_conn, "wtf", default="test") == "test"

    test = get_all_kv_pairs(database_conn)

    assert len(list(test.keys())) == 2
    assert test["foo"] == "bar"
    assert test["baz"] == {"hello": "world"}

    with transaction_wrapper(database_conn) as t:
        remove_key(t, "foo")
        remove_key(t, "baz")

    assert not get_all_kv_pairs(database_conn)
    assert not does_key_exist(database_conn, "foo")
    assert not get_value_by_key(database_conn, "foo")
