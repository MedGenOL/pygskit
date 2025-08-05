[![Python application](https://github.com/MedGenOL/pygskit/actions/workflows/python-app.yml/badge.svg?branch=main)](https://github.com/MedGenOL/pygskit/actions/workflows/python-app.yml)
[![Python Package using Conda](https://github.com/MedGenOL/pygskit/actions/workflows/python-package-conda.yml/badge.svg)](https://github.com/MedGenOL/pygskit/actions/workflows/python-package-conda.yml)

pygskit: A general-purpose Python package for the analysis of genetic data.
--

### Overview
`pygskit` is a Python package designed for the analysis of genetic data using mostly Hail. 
*This package is currently under development.*

### Key features
- Scalable join calling
- QC and filtering
- Computation of cohort-specific metrics (e.g., allele frequency)

### Installing the Required Dependencies via Conda

```bash
git clone https://github.com/MedGenOL/pygskit
cd pygskit
conda env create -f environment.yml
conda activate pygskit
pip install . -r requirements.txt
```

### Updating the package
```bash
cd pygskit
git pull origin main # update the source code from <main> branch
conda env update -f environment.yml # required only if the environment.yml file has been updated
conda activate pygskit
pip install . -r requirements.txt --upgrade
```

### Maintainers
- Enrique Audain
- Rafiga Masmaliyeva
