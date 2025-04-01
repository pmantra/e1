from db.clients import postgres_connector


class TestApplicationConnector:
    def test_application_connectors_singleton(self):
        # when
        connector_1 = postgres_connector.ApplicationConnectors()
        connector_2 = postgres_connector.ApplicationConnectors()

        # verify singleton
        assert connector_1 is connector_2
        assert id(connector_1) == id(connector_2)

    def test_application_connectors_create_pool(self):
        connectors = postgres_connector.application_connectors()
        assert connectors["main"] is not None
        assert connectors["read"] is not None
