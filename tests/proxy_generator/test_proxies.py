from earthquake_data_layer.proxy_generator import ProxiesGenerator


def test_gen():
    proxy_generator = ProxiesGenerator()
    assert proxy_generator.gen()
