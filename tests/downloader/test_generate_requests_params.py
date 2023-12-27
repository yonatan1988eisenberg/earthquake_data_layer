# from earthquake_data_layer import Downloader
# from earthquake_data_layer import definitions
#
#
# def test_default_parameters():
#     result = Downloader.generate_requests_params()
#     assert result == [{'count': definitions.MAX_RESULTS_PER_REQUEST, 'start': 1}]
#
#
# def test_custom_parameters():
#     total_results = 100
#     offset = 5
#     base_params = {'startDate': '2023-01-01'}
#     result = Downloader.generate_requests_params(total_results=total_results, offset=offset, base_params=base_params)
#
#     expected_result = [
#         {'count': total_results, 'start': offset, 'startDate': '2023-01-01'}
#     ]
#
#     assert result == expected_result
#
#
# def test_custom_total_results():
#     total_results = 50
#     result = Downloader.generate_requests_params(total_results=total_results)
#     assert result == [{'count': total_results, 'start': 1}]
#
