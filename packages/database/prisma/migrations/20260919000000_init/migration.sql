CREATE TYPE "GenerationPriority" AS ENUM ('interactive', 'normal', 'background');
CREATE TYPE "GenerationStatus" AS ENUM ('CREATED', 'QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED');

CREATE TABLE "Generation" (
  "id" TEXT NOT NULL,
  "model" TEXT NOT NULL,
  "priority" "GenerationPriority" NOT NULL,
  "status" "GenerationStatus" NOT NULL DEFAULT 'CREATED',
  "prompt" TEXT,
  "promptHash" TEXT,
  "promptLength" INTEGER NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "queuedAt" TIMESTAMP(3),
  "startedAt" TIMESTAMP(3),
  "completedAt" TIMESTAMP(3),
  "failedAt" TIMESTAMP(3),
  "cancelledAt" TIMESTAMP(3),
  "workerId" TEXT,
  "errorMessage" TEXT,
  "tokenCount" INTEGER NOT NULL DEFAULT 0,
  "durationMs" INTEGER,
  "queueWaitMs" INTEGER,
  "timeToFirstTokenMs" INTEGER,
  "retryCount" INTEGER NOT NULL DEFAULT 0,
  CONSTRAINT "Generation_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "ModelDefinition" (
  "name" TEXT NOT NULL,
  "estimatedMemoryMb" INTEGER NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "ModelDefinition_pkey" PRIMARY KEY ("name")
);

CREATE INDEX "Generation_createdAt_idx" ON "Generation"("createdAt" DESC);
CREATE INDEX "Generation_status_priority_idx" ON "Generation"("status", "priority");
