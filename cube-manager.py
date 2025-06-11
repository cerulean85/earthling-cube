import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from earthling.service.ComManager import run

if __name__ == '__main__':
    run()