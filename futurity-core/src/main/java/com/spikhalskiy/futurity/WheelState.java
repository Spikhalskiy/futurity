/*
 * Copyright 2016 Dmitry Spikhalskiy. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * limitations under the License.
 */
package com.spikhalskiy.futurity;

/**
 * Represents state of the Wheel timer
 */
enum WheelState {
    /**
     * Normally functioning wheel which proceed tasks by itself
     */
    ACTIVE,

    /**
     * Wheel in this status migrates tasks to another wheel before dying
     */
    MIGRATING,

    /**
     * This wheel is just shutting down without migration to another wheel
     */
    SHUTDOWN,

    /**
     * This wheel is shutting down because of JVM shutdown hook
     */
    SHUTDOWN_JVM,

    /**
     * This wheel is migrated or shut down
     */
    TERMINATED
}
