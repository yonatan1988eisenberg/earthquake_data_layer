from earthquake_data_layer import Data


def test_latest_update():
    # todo: find a way to do this without overriding data. if the test fails after override we loos the data.
    # save values
    actual_date, actual_offset = Data.get_latest_update()

    # set new values
    expected_date, expected_offset = "1999-03-02", 5
    result = Data.update_latest_update(expected_date, expected_offset)
    assert result

    # assert new values are set
    date, offset = Data.get_latest_update()
    assert (date, offset) == (expected_date, expected_offset)

    # return to initial state
    result = Data.update_latest_update(actual_date, actual_offset)
    assert result

    # verify returned to initial state
    date, offset = Data.get_latest_update()
    assert (date, offset) == (actual_date, actual_offset)


