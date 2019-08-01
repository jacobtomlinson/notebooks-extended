# Credit danielsc
# https://github.com/danielsc/azureml-and-dask/blob/master/dask/startDask.py

from mpi4py import MPI
import os
import argparse
import time
import socket
from azureml.core import Run

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    ip = socket.gethostbyname(socket.gethostname())
    print('My rank is ',rank)
    print('My ip is ', ip)
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--data')
    parser.add_argument('--gpus')
    FLAGS, unparsed = parser.parse_known_args()

    #print('data path', FLAGS.data)
    #os.system('find ' + FLAGS.data + '/nycflights/')

    
    if rank == 0:
        data = {'scheduler' : 'tcp://' + ip + ':8786', 
                'bokeh' : 'tcp://' + ip + ':8787',}
        Run.get_context().log('headnode', ip)
        Run.get_context().log('scheduler', data['scheduler'])
        Run.get_context().log('bokeh', data['bokeh'])
        Run.get_context().log('data', FLAGS.data)
    else:
        data = None
        
    data = comm.bcast(data, root=0)
    scheduler = data['scheduler']
    print('scheduler is ', scheduler)

    
    if rank == 0:
        os.system('dask-scheduler')
    elif rank == 1:
        os.environ['CUDA_VISIBLE_DEVICES'] = '0,1'  # Allow the 1st worker to grab the GPU assigned to the scheduler as well as it's own
        os.system('dask-cuda-worker ' + scheduler + ' --memory-limit 0')
    else:
        os.environ['CUDA_VISIBLE_DEVICES'] = str(rank % int(FLAGS.gpus))  # Restrict each worker to their own GPU (assuming one GPU per worker)
        os.system('dask-cuda-worker ' + scheduler + ' --memory-limit 0')