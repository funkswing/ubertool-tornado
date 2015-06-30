__author__ = 'jflaisha'

import motor, json
from bson import json_util
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, asynchronous


class TestHandler(RequestHandler):
    @asynchronous
    @gen.coroutine
    def get(self):
        db = self.settings['db']
        document = yield db.sam.find_one({"run_type": "single"})
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps((document),default=json_util.default))
        print document


def make_app():
    db = motor.MotorClient().ubertool   # Create single DB instance, and pass that to the Application
    return Application([
        url(r"/", TestHandler),
        ], db=db)


def main():
    app = make_app()
    app.listen(8787)
    # server = tornado.httpserver.HTTPServer(app)
    # server.bind(8888)
    # server.start(0)  # forks one process per cpu (e.g. one tornado Python process per CPU)
    # IOLoop.current().start()
    IOLoop.instance().start()


if __name__ == '__main__':
    main()