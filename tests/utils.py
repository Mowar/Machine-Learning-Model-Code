from tensorflow.python.keras.models import save_model, load_model


def check_model(model, model_name, x, y, category="regression"):

    if category == "regression":
        model.compile('adam', "mean_squared_error", metrics=['mean_absolute_error'])
    elif category == "binary_classification":
        model.compile('adam', 'binary_crossentropy', metrics=['binary_crossentropy'])
    else:
        model.compile('adam', 'categorical_crossentropy', metrics=['accuracy'])

    model.fit(x, y, batch_size=100, epochs=1, validation_split=0.5)

    print(model_name + " test train valid pass!")
    model.save_weights(model_name + '_weights.h5')
    model.load_weights(model_name + '_weights.h5')
    print(model_name + " test save load weight pass!")
    save_model(model, model_name + '.h5')
    model = load_model(model_name + '.h5')
    print(model_name + " test save load model pass!")

    print(model_name + " test pass!")
