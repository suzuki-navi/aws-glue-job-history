from setuptools import setup, find_packages

with open('requirements.txt') as requirements_file:
    install_requirements = requirements_file.read().splitlines()

setup(
    name        = "aws-glue-job-history",
    version     = "0.0.1",
    description = "aws-glue-job-history",
    author      = "suzuki-navi",
    packages    = find_packages(),
    install_requires = install_requirements,
    include_package_data = True,
    entry_points = {
        "console_scripts": [
            "aws-glue-job-history = aws_glue_job_history.main:main",
        ]
    },
)

