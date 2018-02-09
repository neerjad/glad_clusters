from __future__ import print_function
import numpy as np
from skimage import io
from pprint import pprint
import matplotlib.pyplot as plt

SIZE=256


""" convert list of points to image
"""
def data_to_image(data,val=1):
    data=data.astype(int)
    im=np.zeros((SIZE,SIZE))
    nb_bands=data.shape[1]
    if nb_bands==2:
        im[data[:,0],data[:,1]]=val
    elif nb_bands==3:
        im[data[:,0],data[:,1]]=data[:,2]
    else:
        print("TODO: handle data with more than 3 bands")
        raise 
    return im



""" plot list of images in a row
"""
def plot_images(
        images,
        titles=None,
        figsize=(12,4)):
    fig, axs = plt.subplots(1,len(images), figsize=figsize)
    for i,image in enumerate(images):
        if titles: 
            title=titles[i]
        else:
            title=None
        ax=axs[i]
        ax.imshow(image)
        ax.set_title(title)
    plt.show()



""" plot cluster
    - plot data from single cluster
    - mark cluster centroid with dot
"""
def plot_cluster(cluster_data):
    info=cluster_data.copy()
    alerts=info.pop('alerts')
    pprint(info)
    cluster=data_to_image(alerts)
    fig, ax = plt.subplots(1,1, figsize=(4,4))
    ax.imshow(cluster)
    ax.scatter([info['j']],[info['i']],marker='o',c='r',s=20)
    plt.show()


""" plot multiple clusters in a row
"""
def plot_clusters_row(clusters):
    nb_clusters=len(clusters)
    fig, axs = plt.subplots(1,nb_clusters, figsize=(18,3))
    for i in range(nb_clusters):
        ax=axs[i]
        info=clusters[i].copy()
        alerts=info.pop('alerts')
        title='area:{}, count:{}, pt:{},{}'.format(
                info['area'],
                info['count'],
                info['i'],
                info['j'])
        subtitle='dates: {}-{}'.format(
                info['min_date'],
                info['max_date'])        
        cluster=data_to_image(alerts)
        ax.imshow(cluster)
        ax.scatter([info['j']],[info['i']],marker='o',c='r',s=20)
        ax.set_title(title)
        ax.set_xlabel(subtitle)
    plt.show()


""" plot cluster centroids over input data
"""
def plot_clusters(clusters_data,figsize=(4,4)):
    print("NB CLUSTERS:",clusters_data['nb_clusters'])
    input_data=data_to_image(clusters_data['input_data'])
    clusters=np.array([[c['i'],c['j']] for c in clusters_data['clusters']])
    fig, ax = plt.subplots(1,1, figsize=figsize)
    ax.imshow(input_data)
    ax.scatter(clusters[:,1],clusters[:,0],marker='o',c='r',s=20)
    ax.set_xlim([0,255])
    ax.set_ylim([255,0])


