# Release Checklist

- [ ] Get `main` to the appropriate code release state.
      [GitHub Actions](https://github.com/tussikcom/tussikzpl/actions) should be running
      
* [ ] Start from a freshly cloned repo:

```bash
cd /tmp
rm -rf tussik-dyno
git clone https://github.com/tussikcom/tussik-dyno
cd tussik-dyno
```

* [ ] (Optional) Create a distribution and release on **TestPyPI**:

```bash
make clean
make run
```

- [ ] (Optional) Check **test** installation:

```bash
make prerelease
```

* [ ] Tag with the version number:

```bash
git tag -a 0.1.0.1 -m "Release 0.1.0.1"
```

* [ ] Create a distribution and release on **live PyPI**:

```bash
make release
```

* [ ] Check installation:

```bash
pip uninstall -y tussik.dyno
pip install -U tussik.dyno
python3 -c "import tussik.dyno; print(tussik.dyno.__version__)"
```

* [ ] Push tag:
 ```bash
git push --tags
```

* [ ] Edit release draft, adjust text if needed: https://github.com/tussikcom/tussik-dyno/releases

* [ ] Check next tag is correct, amend if needed

* [ ] Publish release