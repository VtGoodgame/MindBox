from setuptools import setup, find_packages


def readme():
  with open('README.md', 'r') as f:
    return f.read()


setup(
  name='geometry',
  version='0.0.1',
  author='Vtgoodgame',
  author_email='devisheva.ina@gmail.com',
  description='Библиотека для расчета площади и периметра фигур: круг, треугольник',
  long_description=readme(),
  long_description_content_type='text/markdown',
  url='https://github.com/VtGoodgame/MindBox',
  packages=find_packages(),
  keywords='calculate area perimeter circle triangle square',
  project_urls={
    'GitHub': 'https://github.com/VtGoodgame'
  },
  python_requires='>=3.12'
)