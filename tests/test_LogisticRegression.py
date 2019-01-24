import pytest
from models import LogisticRegression
from sklearn.datasets import make_classification
from tests.utils import check_model


@pytest.mark.parametrize(
    'dim',
    [8, 10, 5]
)
def test_LogisticRegression(dim):
    model_name = "LogisticRegression"

    x, y = make_classification(n_samples=1000, n_features=dim)

    model = LogisticRegression(dim)
    check_model(model, model_name, x, y, category="binary_classification")
