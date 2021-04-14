import unittest

class SequentialTest(unittest.TestCase):
    def _steps(self):
        for name in dir(self):
            if name.startswith('step'):
                yield name, getattr(self, name)

    def run_tests(self):
        for name, step in self._steps():
            try:
                print('Running test {}'.format(name))
                step()
                print('Test {} passed\n'.format(name))
            except Exception as e:
                self.fail("{} failed ({}: {})".format(step, type(e), e))
        print('All tests passed')
