"""This is a small snipper of the test suite from pyarrow.flight (which uses grpc-cpp and mixing it with a native check

Adapted from:
https://github.com/apache/arrow/blob/apache-arrow-8.0.0/python/pyarrow/tests/test_flight.py
"""

import pyarrow as pa
import pytest
import grpc
from pyarrow import flight
from pyarrow.flight import FlightServerBase


def simple_ints_table():
    data = [
        pa.array([-10, -5, 0, 5, 10])
    ]
    return pa.Table.from_arrays(data, names=['some_ints'])


def simple_dicts_table():
    dict_values = pa.array(["foo", "baz", "quux"], type=pa.utf8())
    data = [
        pa.chunked_array([
            pa.DictionaryArray.from_arrays([1, 0, None], dict_values),
            pa.DictionaryArray.from_arrays([2, 1], dict_values)
        ])
    ]
    return pa.Table.from_arrays(data, names=['some_dicts'])


def multiple_column_table():
    return pa.Table.from_arrays([
        pa.array(['foo', 'bar', 'baz', 'qux']),
        pa.array([1, 2, 3, 4])],
        names=['a', 'b']
    )


class ConstantFlightServer(FlightServerBase):
    """A Flight server that always returns the same data.
    See ARROW-4796: this server implementation will segfault if Flight
    does not properly hold a reference to the Table object.
    """

    CRITERIA = b"the expected criteria"

    def __init__(self, location=None, options=None, **kwargs):
        super().__init__(location, **kwargs)
        # Ticket -> Table
        self.table_factories = {
            b'ints': simple_ints_table,
            b'dicts': simple_dicts_table,
            b'multi': multiple_column_table,
        }
        self.options = options

    def list_flights(self, context, criteria):
        if criteria == self.CRITERIA:
            yield flight.FlightInfo(
                pa.schema([]),
                flight.FlightDescriptor.for_path('/foo'),
                [],
                -1, -1
            )

    def do_get(self, context, ticket):
        # Return a fresh table, so that Flight is the only one keeping a
        # reference.
        table = self.table_factories[ticket.ticket]()
        return flight.RecordBatchStream(table, options=self.options)


@pytest.fixture
def server():
    # This code doesn't do much but makes sure the native extension is loaded
    # which is what we are testing here.
    channel = grpc.insecure_channel('localhost:10000')
    with ConstantFlightServer() as server:
        yield server
    del channel


def test_flight_list_flights(server):
    """Try a simple list_flights call."""
    with flight.connect(('localhost', server.port)) as client:
        assert list(client.list_flights()) == []
        flights = client.list_flights(ConstantFlightServer.CRITERIA)
        assert len(list(flights)) == 1


def test_flight_client_close(server):
    with flight.connect(('localhost', server.port)) as client:
        assert list(client.list_flights()) == []
        client.close()
        client.close()  # Idempotent
        with pytest.raises(pa.ArrowInvalid):
            list(client.list_flights())

