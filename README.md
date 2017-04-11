# futurity [![Build Status](https://travis-ci.org/Spikhalskiy/futurity.svg?branch=master)](https://travis-ci.org/Spikhalskiy/futurity) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.spikhalskiy.futurity/futurity-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.spikhalskiy.futurity/futurity-core)
A simple tool to convert plain old Java Future to CompletableFuture

## How to use?

```
<dependency>
    <groupId>com.spikhalskiy.futurity</groupId>
    <artifactId>futurity-core</artifactId>
    <version>0.3-RC3</version>
</dependency>
```

```java
Future oldFuture = ...;
CompletableFuture profit = Futurity.shift(oldFuture);
```

## Motivation

You have an old code, which is asynchronous, but uses pre Java 8 API
and you want to convert Future to CompletableFuture to get a full power
of the new api.

It could require an update of 3rd party dependencies.
And even in this case it could be a bit tricky
if a library that performs IO doesn't support nor CompletableFuture 
neither callback API.
If you have large amount of own code under Future -
it could require big refactoring to introduce 
callbacks or a CompletableFuture model.

The best that you can do without large immediate reworking from 
related [StackOverflow discussion](https://stackoverflow.com/questions/23301598/transform-java-future-into-a-completablefuture) is:

```java
public static <T> CompletableFuture<T> makeCompletableFuture(Future<T> future) {
    return CompletableFuture.supplyAsync(() -> {
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw new CompletionException(e.getCause());
        } catch (InterruptedException e) {
            throw new CompletionException("Interrupted");     
        }
    });
}
```

Which is very bad solution, because a thread will be blocked and wait
for the Future result. What if it's a thread from common pool?
Mostly possible you didn't pass special executor. What if you have a lot
of such futures? Thread pool could be quickly exhausted and you steal
common resources from useful work for just active future checking. 

This library provides a better way - it maintains collections of
passed futures inside and returns CompletableFuture outside that gets
value when Future would be done. For checking all passed futures
futurity uses only one thread.

Futurity could be a good choice for a migration and to be a mediator
between new code that wants to use all features provided by
CompletableFuture (CompletionStage) and old code, which supports plain
Future only. Futurity is either good for tests code where you don't want
to spend much time for supporting CompletableFuture properly and want to
leave code with Futures and use it outside in CompletableFuture way.

## Caution

You shouldn't consider this library to create new good things and new APIs.
Inside this library you still have active waiting on futures and waste
resources on it. Futurity just makes it much cheaper than
straightforward active waiting on each future in
CompletableFuture.supplyAsync. Plan your new code right and use
appropriate underlying libraries that doesn't require active checking
of futures by supporting callbacks of CompletableFuture APIs.
