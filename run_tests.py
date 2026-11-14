#!/usr/bin/env python3
"""隔离模式（python -I）下的测试入口。

`python -I` 不把当前目录加入 sys.path，因此在运行前显式注入项目根，
保证测试不会拾取到工作区外的同名包。
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = loader.discover(os.path.join(os.path.dirname(
        os.path.abspath(__file__)), 'tests'))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
