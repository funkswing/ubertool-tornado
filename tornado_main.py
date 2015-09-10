__author__ = 'jflaisha'

import motor, json, cPickle
from bson import json_util
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, asynchronous


class TestHandler(RequestHandler):
    @asynchronous
    @gen.coroutine
    def get(self):
        db = self.settings['db']
        document = yield db.sam.find_one({ "run_type": "single" })
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps((document),default=json_util.default))
        print document


class SamDailyHandler(RequestHandler):
    """
    /sam/daily/<jid>
    """
    @asynchronous
    @gen.coroutine
    def get(self, jid):
        print jid
        db = self.settings['db']
        document = yield db.sam.find_one({ "jid": jid })
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps((document),default=json_util.default))

    @asynchronous
    @gen.coroutine
    def post(self, jid):
        db = self.settings['db']
        try:
            print 'JSON'
            document = json.loads(self.request.body)
        except ValueError:
            print 'Pickle'
            document = cPickle.loads(self.request.body)

        yield db.sam.insert(document)
        self.set_header("Content-Type", "application/json")
        self.set_status(201)

def make_app():
    db = motor.MotorClient().ubertool   # Create single DB instance, and pass that to the Application
    return Application([
        url(r"/", TestHandler),
        (r'/sam/daily/(.*)', SamDailyHandler)
        ], db=db)


def main():
    app = make_app()
    app.listen(8787)
    # server = tornado.httpserver.HTTPServer(app)
    # server.bind(8888)
    # server.start(0)  # forks one process per cpu (e.g. one tornado Python process per CPU)
    IOLoop.current().start()
    # IOLoop.instance().start()


if __name__ == '__main__':
    main()