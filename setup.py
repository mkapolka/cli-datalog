from setuptools import setup

setup(name='cli-datalog',
      version='0.1',
      description='Lil DIY library for making your own datalog tool with your personal data source.',
      url='http://github.com/mkapolka/cli-datalog',
      author='Marek Kapolka',
      author_email='marek.kapolka@gmail.com',
      license='Copyright 2019 Marek Kapolka... for now!',
      packages=['cli_datalog'],
      install_requires=['pandas', 'pyparsing'],
      zip_safe=True)


