import pytest
from models import SoftmaxRegression
from sklearn.datasets import make_multilabel_classification
from tests.utils import check_model


@pytest.mark.parametrize(
    'feat_num,cate_num',
    [(8, 2), (10, 5), (5, 3)]
)
def test_SoftmaxRegression(feat_num, cate_num):
    model_name = "SoftmaxRegression"

    x, y = make_multilabel_classification(n_samples=1000, n_features=feat_num, n_classes=cate_num)

    model = SoftmaxRegression(feat_num, cate_num)
    check_model(model, model_name, x, y, category="multi_classification")
