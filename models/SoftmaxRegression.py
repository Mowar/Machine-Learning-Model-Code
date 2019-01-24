from tensorflow.python.keras.models import Model
from tensorflow.python.keras.layers import Input, Dense


def SoftmaxRegression(feat_num, cate_num):
    """
        feat_num: int,特征的维度
        cate_num: int, 类别的数目
    """
    input_ = Input(shape=(feat_num,), name="input_name")
    output = Dense(units=cate_num, activation="softmax")(input_)

    model = Model(inputs=[input_], outputs=[output])

    return model
