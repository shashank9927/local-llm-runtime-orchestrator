import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();
await prisma.modelDefinition.createMany({
  data: [
    { name: 'mock:latest', estimatedMemoryMb: 128 },
    { name: 'llama3.2:3b', estimatedMemoryMb: 2500 },
    { name: 'qwen2.5:3b', estimatedMemoryMb: 2800 },
  ],
  skipDuplicates: true,
});
await prisma.$disconnect();
