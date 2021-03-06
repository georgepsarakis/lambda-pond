from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

setup(
    name="lambda-pond",
    version="0.0.1",
    author="George Psarakis",
    description="Process Pool without semaphore and shared memory use",
    long_description=readme,
    long_description_content_type="text/markdown",
    package_dir={"": "."},
    packages=find_packages(),
    zip_safe=False,
    install_requires=[],
    extras_require={
        "dev": [
            "pytest==6.1.0",
            "flake8==3.8.4",
            "pytest-cov==2.10.1"
        ]
    },
    url="https://github.com/georgepsarakis/lambda-pond",
    license="MIT",
    include_package_data=True,
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Operating System :: POSIX :: Linux",
        "Development Status :: 2 - Pre-Alpha",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
)
