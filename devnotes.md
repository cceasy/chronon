## Commands

***All commands assume you are in the root directory of this project***.
For me, that looks like `~/workspace/chronon`.

## Prerequisites
1. Install Thrift (e.g. via `brew install thrift` on macOS. Version 0.22 is recommended)
2. Install Java. Ideally version 11, e.g. via `brew install openjdk@11` on macOS - newer versions of Java can run into issues due to stricter Java platform module system checks particularly with engines like Spark / Flink)
3. Install Scala 2.12
4. Install Python 3.11 or higher

## Using a Plugin Manager
You can use a plugin manager like [asdf](https://asdf-vm.com/guide/getting-started.html#_2-download-asdf)
* Install [asdf](https://asdf-vm.com/guide/getting-started.html#_2-download-asdf)
* ```asdf plugin add asdf-plugin-manager```
* ```asdf install asdf-plugin-manager latest```
* ```asdf exec asdf-plugin-manager add-all``` (see `.plugin-versions` for required plugins)
* ```asdf exec asdf-plugin-manager update-all```
* ```asdf install``` (see `.tool-versions` for required runtimes and versions)

## Build system
We use [mill](https://mill-build.org/mill/index.html) for building Scala and Python code.

### Why mill?
mill has a couple of nice side effects:
1. Builds are very fast - they are cached and executed in parallel
2. Good Python support - comes with
   a. ruff linting
   b. pytest support
   c. wheel
   d. pex bundling
   e. pypi uploads for free  (literally 20 lines of code to get all our py workflow into mill)
3. Easy to reason about dependencies and modules
   a. mill is a significant reduction in code size and indirection
   b. the ide click-into and auto complete works even with build files
4. Much smaller build output sizes (e.g output size for spark dropped from 480MB to 62MB)
   a. simply because we can mark spark deps as “provided” that only are available for compilation but not for assembly or runtime.

### Useful scala commands
```
# reformat scala code
./mill __.reformat

# compile scala code
./mill __.compile

# build assembly jar
./mill __.assembly

# run specific tests that match a pattern etc
./mill spark.test.testOnly "ai.chronon.spark.analyzer.*"

# run a particular test case inside a test class
./mill spark.test.testOnly "ai.chronon.spark.kv_store.KVUploadNodeRunnerTest" -- -z "should handle GROUP_BY_UPLOAD_TO_KV successfully"

# run all tests of a sub-module
./mill api.test

# to find where a dependency comes from
./mill spark.showMvnDepsTree --whatDependsOn com.google.protobuf:protobuf-java

# show actions available for a given module
./mill resolve spark.__
```

### useful python commands
```
# reformat python code
./mill python.ruffCheck --fix

# run python tests
./mill python.test

# build the wheel - the user needs to have python installed
./mill python.wheel

# build an editable package (for ide and development)
./mill python.installEditable

# build and install current wheel
./mill python.installWheel

# build and run the entry point (zipline.py)
./mill python.run hub backfill ...

# run coverage
./mill python.test.coverageReport --omit='**/test/**,**/out/**'

# publish to pypi
# ask nikhil to generate a token for you
export MILL_TWINE_REPOSITORY_URL=https://pypi.org/
export MILL_TWINE_USERNAME=__token__
export MILL_TWINE_PASSWORD=<apitoken> 
./mill python.publish

# build self contained python binary - include python and all deps (transitive included) into a single file
# uses pex under the hood
./mill python.bundle
```

### project setup

Mill build spans across a central build.mill and a per module package.mill file.

Here is a simple but real example

```scala
object `package` extends build.BaseModule {
  def moduleDeps = Seq(build.api)

  // equivalent to "provided" , meaning the package is only available for compile
  // but at runtime it needs to come through from class path.
  def compileMvnDeps = Seq(
    mvn"org.apache.spark::spark-sql:${build.Constants.sparkVersion}"
  )
  
  // the actual dependencies
  def mvnDeps = build.Constants.commonDeps ++ build.Constants.loggingDeps ++ build.Constants.utilityDeps ++ Seq(
    mvn"org.apache.datasketches:datasketches-memory:3.0.2",
    mvn"org.apache.datasketches:datasketches-java:6.1.1",
  )
  
  // test setup
  object test extends build.BaseTestModule {
    def moduleDeps = Seq(build.aggregator, build.api.test)
    def mvnDeps = super.mvnDeps() ++ build.Constants.testDeps
  }
}
```

## Configuring IntelliJ IDEA
* Install the Build Server Protocol (BSP) plugin
* From the menu, select File -> New -> Project from Existing Sources...
* Select the root directory of the project
* Select "Import project from external model" and choose "BSP"

## Pushing code

Our CI pipeline runs scalafmt checks on scala code before allowing merges.
You can either add a git pre-commit hook to run scalafmt before every commit, or use the following alias and function to format, commit, and push in one command.

```sh
alias mill_scalafmt='./mill __.reformat'

function zpush() {
    if [ $# -eq 0 ]; then
        echo "Error: Please provide a commit message."
        return 1
    fi

    local commit_message="$1"

    mill_scalafmt && \
    git add -u && \
    git commit -m "$commit_message" && \
    git push

    if [ $? -eq 0 ]; then
        echo "Successfully compiled, formatted, committed, and pushed changes."
    else
        echo "An error occurred during the process."
    fi
}
```

You can invoke this command as below

```
zpush "Your commit message"
```

> Note: The quotes are necessary for multi-word commit message.