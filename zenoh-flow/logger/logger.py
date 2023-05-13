import time
import argparse
import json
import zenoh
from zenoh import Reliability, Sample


class Logger(object):
    def __init__(self, params):
        # initiate logging
        zenoh.init_logger()

        self.params = params

        self.conf = zenoh.Config()
        if self.params.mode is not None:
            self.conf.insert_json5(zenoh.config.MODE_KEY, json.dumps(self.params.mode))
        if self.params.connect is not None:
            self.conf.insert_json5(zenoh.config.CONNECT_KEY, json.dumps(self.params.connect))
        if self.params.listen is not None:
            self.conf.insert_json5(zenoh.config.LISTEN_KEY, json.dumps(self.params.listen))

        self.key = self.params.key

        self.session = zenoh.open(self.conf)

        self.file = open(self.params.output, "w+")

        self.sub = self.session.declare_subscriber(self.key, self.on_sample, reliability=Reliability.RELIABLE())
        self.is_running = True

    def on_sample(self, sample: Sample):
        payload = json.loads(sample.payload.decode("utf-8"))

        if "shutdown" in payload:
            self.is_running = False
        else:
            self.file.write(json.dumps(payload))
            self.file.flush()

    def run(self):

        while self.is_running:
            time.sleep(1)

        self.file.clone()
        self.sub.undeclare()
        self.session.close()


if __name__ == '__main__':
    # --- Command line argument parsing --- --- --- --- --- ---
    parser = argparse.ArgumentParser(
        prog='z_sub',
        description='zenoh sub example')
    parser.add_argument('--mode', '-m', dest='mode',
                        choices=['peer', 'client'],
                        type=str,
                        help='The zenoh session mode.')
    parser.add_argument('--connect', '-e', dest='connect',
                        metavar='ENDPOINT',
                        action='append',
                        type=str,
                        help='Endpoints to connect to.')
    parser.add_argument('--listen', '-l', dest='listen',
                        metavar='ENDPOINT',
                        action='append',
                        type=str,
                        help='Endpoints to listen on.')
    parser.add_argument('--key', '-k', dest='key',
                        default='demo/example/**',
                        type=str,
                        help='The key expression to subscribe to.')
    parser.add_argument('--file', '-f', dest='output',
                        metavar='FILE',
                        type=str,
                        help='The output file.')

    args = parser.parse_args()

    s = Logger(args)
    s.run()