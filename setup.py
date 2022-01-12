import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='lcfsLambdaLib',
    author='Dan Herrington',
    author_email='dan@cascadiarc.com',
    version='0.0.15',
    packages=[
        'lcfsLambdaLib',
    ],
    url='https://github.com/cascadiarc/lcfsLambdaLib',
    license='MIT License',
    description='Library to hold commonly used aws functions for Lambda',
    long_description=open('README.md').read(),
)