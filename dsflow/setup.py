import setuptools


setuptools.setup(
    name='dsflow',
    version='0.0.1',
    install_requires=[
        "google",
        "protobuf",  # see https://stackoverflow.com/questions/38680593/importerror-no-module-named-google-protobuf
    ],
    packages=setuptools.find_packages(),
)
