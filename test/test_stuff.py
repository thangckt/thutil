from thutil.stuff import unpack_indices


def test_unpack_indices():
    """Test if the index expansion works as expected"""
    list_inputs = [1, 2, "3-5:2", "6-10"]
    idx_exp = unpack_indices(list_inputs)
    assert idx_exp == [1, 2, 3, 5, 6, 7, 8, 9, 10]
