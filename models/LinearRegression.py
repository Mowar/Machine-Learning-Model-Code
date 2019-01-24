from tensorflow.python.keras.models import Model
from tensorflow.python.keras.layers import Input, Dense


def LinearRegression(dim):
    """
        dim: int,特征的维度
    """
    input_ = Input(shape=(dim,), name="input_name")
    output = Dense(units=1, activation="linear")(input_)

    model = Model(inputs=[input_], outputs=[output])

    return model
