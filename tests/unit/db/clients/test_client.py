from unittest import mock

from aiosql import queries

from db.clients import client


class TestQueryLoader:
    def test_queryloader_singleton(self):
        # when
        loader_1 = client.QueryLoader()
        loader_2 = client.QueryLoader()

        # verify singleton
        assert loader_1 is loader_2
        assert id(loader_1) == id(loader_2)

    def test_queryloader_load_once(self):
        mock_queries = mock.Mock(spec=queries.Queries)
        with mock.patch(
            "aiosql.from_path", return_value=mock_queries
        ) as mock_from_path:
            res_1 = client.QueryLoader().load("mocked")
            res_2 = client.QueryLoader().load("mocked")
            assert res_1 == res_2
            assert res_1 == mock_queries
            assert mock_from_path.call_count == 1
