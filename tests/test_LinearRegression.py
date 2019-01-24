import pytest
from models import LinearRegression
from sklearn.datasets import make_regression
from tests.utils import check_model


@pytest.mark.parametrize(
    'dim',
    [8, 10, 5]
)
def test_LinearRegression(dim):
    model_name = "LinearRegression"

    x, y = make_regression(n_samples=1000, n_features=dim)

    model = LinearRegression(dim)
    check_model(model, model_name, x, y, category="regression")

