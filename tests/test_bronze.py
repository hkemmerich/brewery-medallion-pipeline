import pytest

def validate_bronze(records):
    if len(records) == 0:
        raise ValueError('A camada bronze está vazia')
    
def test_bronze_falha_quando_vazia():
    with pytest.raises(ValueError):
            validate_bronze([])

def test_bronze_regis():
     records = [{'id': '1', 'name': 'Brewery A'}]
     validate_bronze(records)
     assert len(records) == 1