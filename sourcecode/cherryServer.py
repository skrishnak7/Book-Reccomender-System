import  cherrypy, os
from paste.translogger import TransLogger
from flaskApp import create_app
from pyspark import SparkContext, SparkConf
def init_spark_context():
    conf = SparkConf().setAppName("movie_recommendation-server")
    sc = SparkContext(conf=conf, pyFiles=['engine.py', 'flaskApp.py'])
    return sc
def run_server(app):
    app_logged = TransLogger(app)
    cherrypy.tree.graft(app_logged, '/')

    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5070,
        'server.socket_host': '127.0.0.1'
    })
    cherrypy.engine.start()
    cherrypy.engine.block()
if __name__ == "__main__":
    sc = init_spark_context()
    dataset_path = os.path.join('datasets', 'app')
    app = create_app(sc, dataset_path)
    run_server(app)