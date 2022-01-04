from distutils.core import setup

setup(
    name='lcfsLambdaLib',
    author='Dan Herrington',
    author_email='dan@cascadiarc.com',
    version='0.7',
    packages=[
        'lcfsLambdaLib',
    ],
    url='https://github.com/cascadiarc/lcfsLambdaLib',
    license='MIT License',
    description='Library to hold commonly used aws functions for Lambda',
    long_description=open('README.md').read(),
)