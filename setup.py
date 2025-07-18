from setuptools import setup, find_packages

setup(
	name='blaster-server',
	packages=find_packages() + ["blaster/utils/data"],
	version="0.1.51b",
	license='MIT',
	description='Gevent based python server built from scratch for maximum performance',
	author='Abhinav Reddy',                   # Type in your name
	author_email='abhinavabcd@gmail.com',      # Type in your E-Mail
	url='https://github.com/abhinavabcd/blaster',
	download_url='https://github.com/abhinavabcd/blaster/archive/v0.0337b.tar.gz',
	keywords=['server', 'superfast', 'Like FastApi or Flask but 10x faster'],
	include_package_data=True,
	install_requires=[            # I get to this in a second
		"wheel>=0.34.2",
		"pytz>=2020.1",
		"gevent>=22.9.1",
		"greenlet>=3.0.0",
		"pymongo>=3.12.0",
		"ujson>=5.10.0",
		"python-dateutil>=2.8.1",
		"requests>=2.25.1",
		"requests-toolbelt>=1.0.0",
		"urllib3>=1.26.4",
		"cityhash>=0.4.8",
		"PyYAML>=6.0"
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
	]
)
