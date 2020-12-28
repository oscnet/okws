import getopt
import logging
import os
import os.path
import sys

from yaml import Loader, load
from .main import run

logger = logging.getLogger(__name__)


def usage():
    print('python -m signals.server -c <configfile>')
    exit(1)


def read_config(config_file):
    logger.info(f"config file: {config_file}")
    if os.path.isfile(config_file) and os.access(config_file, os.R_OK):
        with open(config_file, 'r') as f:
            return load(f, Loader=Loader)
    else:
        logger.error(f"{config_file} 文件不存在或不可读")
        exit(1)


def parse_argv(argv):
    try:
        opts, _args = getopt.getopt(argv[1:], "-h-c:", ['help'])
    except getopt.GetoptError:
        usage()

    for opt, arg in opts:
        if opt in ("-c"):
            return read_config(arg)
        else:
            usage()
    usage()


def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(module)s[%(lineno)d] - %(levelname)s: %(message)s')

    # logger.info(sys.argv)
    config = parse_argv(sys.argv)
    # logger.info(config)
    run(config)


if __name__ == '__main__':
    main()