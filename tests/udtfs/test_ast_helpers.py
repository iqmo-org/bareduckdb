import bareduckdb


def test_find_nodes_by_type_dict_match(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:ast_dict_{thread_index}_{iteration_index}")

    ast = {
        "type": "SELECT",
        "children": [
            {"type": "BASE_TABLE", "name": "foo"},
            {"type": "COLUMN", "name": "x"}
        ]
    }

    results = conn._find_nodes_by_type(ast, "BASE_TABLE")

    assert len(results) == 1
    assert results[0]["name"] == "foo"
    conn.close()


def test_find_nodes_by_type_list(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:ast_list_{thread_index}_{iteration_index}")

    ast = [
        {"type": "BASE_TABLE", "name": "t1"},
        {"type": "BASE_TABLE", "name": "t2"}
    ]

    results = conn._find_nodes_by_type(ast, "BASE_TABLE")

    assert len(results) == 2
    conn.close()


def test_find_nodes_by_type_nested(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:ast_nested_{thread_index}_{iteration_index}")

    ast = {
        "type": "SELECT",
        "from": {
            "type": "JOIN",
            "left": {"type": "BASE_TABLE", "name": "t1"},
            "right": {"type": "BASE_TABLE", "name": "t2"}
        }
    }

    results = conn._find_nodes_by_type(ast, "BASE_TABLE")

    assert len(results) == 2
    conn.close()


def test_find_nodes_by_type_no_match(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:ast_nomatch_{thread_index}_{iteration_index}")

    ast = {"type": "SELECT"}
    results = conn._find_nodes_by_type(ast, "NONEXISTENT")

    assert len(results) == 0
    conn.close()


def test_find_nodes_by_type_scalar(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:ast_scalar_{thread_index}_{iteration_index}")

    assert conn._find_nodes_by_type("string", "BASE_TABLE") == []
    assert conn._find_nodes_by_type(123, "BASE_TABLE") == []
    assert conn._find_nodes_by_type(None, "BASE_TABLE") == []
    conn.close()
