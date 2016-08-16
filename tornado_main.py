import sys

__author__ = 'jflaisha'

import motor, json
from bson import json_util
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, asynchronous

# Import model dependencies
from sam import handlers as sam_handlers

class TestHandler(RequestHandler):
    @asynchronous
    @gen.coroutine
    def get(self):
        db = self.settings['db_sam']
        document = yield db.metadata.find_one({})
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps((document),default=json_util.default))
        print document


def make_app(host, port):
    client = motor.MotorClient(host, port)  # ('localhost', 27017)
    # Create single DB instance for each database, and pass that to the Application settings
    db_uber = client.ubertool
    db_sam = client.sam
    return Application([
        url(r"/", TestHandler),
        (r'/sam/metadata/(.*)', sam_handlers.SamMetaDataHandler),
        (r'/sam/daily/(.*)', sam_handlers.SamDailyHandler)
        ],
        # Settings dictionary
        db_uber=db_uber,
        db_sam=db_sam
    )


def main(host, port):

    app = make_app(host, port)
    app.listen(8787)
    # server = tornado.httpserver.HTTPServer(app)
    # server.bind(8888)
    # server.start(0)  # forks one process per cpu (e.g. one tornado Python process per CPU)
    IOLoop.current().start()
    # IOLoop.instance().start()


if __name__ == '__main__':

    try:
        host = sys.argv[1]
    except IndexError:
        host = 'localhost'
    try:
        port = sys.argv[2]
    except IndexError:
        port = 27017

    print "Host: %s \nPort: %s" % (host, port)

    main(host, port)
