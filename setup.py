from setuptools import setup, find_packages

setup(
	name='blaster-server',
	packages=find_packages("."),
	version='0.0.323b',
	license='MIT',
	description='Gevent based python server built from scratch',
	author='Abhinav Reddy',                   # Type in your name
	author_email='abhinavabcd@gmail.com',      # Type in your E-Mail
	url='https://github.com/abhinavabcd/blaster',
	download_url='https://github.com/abhinavabcd/blaster/archive/v0.03.tar.gz',
	keywords=['server', 'superfast', 'just like flask but minimal and fast'],
	install_requires=[            # I get to this in a second
		"wheel>=0.34.2",
		"boto3>=1.13.19",
		"botocore>=1.16.19",
		"pytz>=2020.1",
		"python-dateutil>=2.8.1",
		"gevent>=20.9.0",
		"greenlet>=0.4.16",
		"pymongo>=3.10.1",
		"elasticsearch>=6.1.1",
		"ujson>=2.0.3",
		"python-dateutil>=2.8.1",
		"requests>=2.23.0",
		"requests-toolbelt>=0.9.1",
		"requests-aws4auth>=0.9",
		"PyMySQL>=0.9.3",
	],
	classifiers=[
						'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
						'Intended Audience :: Developers',      # Define that your audience are developers
						'Topic :: Software Development :: Build Tools',
						'License :: OSI Approved :: MIT License',   # Again, pick a license
						'Programming Language :: Python :: 3',      # Specify which pyhton versions that you want to support
						'Programming Language :: Python :: 3.4',
						'Programming Language :: Python :: 3.5',
						'Programming Language :: Python :: 3.6',
	],
)