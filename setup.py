from setuptools import setup, find_packages

setup(
    name="mdt-welder",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "python-dateutil",
        "pyyaml",
    ],
    entry_points={
        'console_scripts': [
            'append-ampere-record=scripts.append_ampere_record:main',
            'inspect-waveform=scripts.inspect_waveform:main',
        ],
    },
) 