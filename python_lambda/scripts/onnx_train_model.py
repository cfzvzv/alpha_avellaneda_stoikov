import os
import numpy as np
import sys
import tensorflow as tf
from tensorflow.keras import Model
from configuration import logger
import onnxruntime as rt
import onnx
import onnxmltools
import onnxmltools.convert.common.data_types



def read_matrix(file:str)->np.ndarray:
    return np.loadtxt(fname=file,delimiter=" ")


def create_model(input, target)->Model:
    from tensorflow.keras.layers import Dense, Lambda, Input
    assert len(input)==len(target)
    loss = 'mse'
    optimizer='sgd'

    input_layer_neurons = input.shape[1]
    output_layer_neurons=target.shape[1]

    hidden_layer_neurons = (
                                   input_layer_neurons + output_layer_neurons
                           ) * 2

    input = Input(shape=(input_layer_neurons,))
    hidden_layer_neuron_1 = Dense(hidden_layer_neurons, activation='tanh', name='hidden_1')(input)
    output = Dense(output_layer_neurons, activation='linear', name='output')(hidden_layer_neuron_1)
    model = Model(input, output, name='ml_onnx_model')
    model.compile(
        optimizer=optimizer,
        loss=loss,
        # metrics=metrics,

    )

    print('Created nn of %d inputs and %d target and middle layer of %d  hidden neurons'% (input_layer_neurons, output_layer_neurons, hidden_layer_neurons))

    return model

def convert_to_onnx(model,input_len,output_len,filepath):
    # https://github.com/onnx/onnxmltools

    initial_types = [("input", onnxmltools.FloatTensorType([0, input_len]))]
    from tf2onnx.tfonnx import process_tf_graph

    input_names=['state_%s'%str(i) for i in range(input_len)]
    output_names = ['score_action_%s' % str(i) for i in range(output_len)]
    with tf.Session() as sess:
        onnx_graph = process_tf_graph(sess.graph, input_names=input_names, output_names=output_names)

    onnx_model = onnxmltools.convert_tensorflow(model,input_names=input_names,output_names=output_names,target_opset=7)
    onnxmltools.utils.save_model(onnx_model, filepath)

def test_onnx(onnx_file,input,tf_model):
    tf_prediction=tf_model.predict(input)
    sess = rt.InferenceSession(onnx_file)

    # get model metadata to enable mapping of new input to the runtime model.
    input_name = sess.get_inputs()[0].name
    label_name = sess.get_outputs()[0].name
    pred_onx = sess.run([label_name], {input_name: input})




if __name__ == '__main__':
    print(f"Arguments count: {len(sys.argv)}")
    arguments=sys.argv[1:]
    if(len(arguments)!=3):
        print (f"wrong number of arguments !=3   {len(arguments)}")
        sys.exit(-1)

    input_file = arguments[0]
    target_file=arguments[1]
    model_file=arguments[2]

    input = read_matrix(input_file)
    target=read_matrix(target_file)

    model=create_model(input,target)
    model.fit(x=input,y=target)



    ### change model to onnx and persist it https://github.com/onnx/onnxmltools
    convert_to_onnx(model=model,filepath=model_file,input_len=input.shape[1],output_len=target.shape[1])
