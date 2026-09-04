import { NestFactory } from '@nestjs/core';
import { CampaignProjectionWorkerModule } from './worker/campaign-projection-worker.module';

async function bootstrap() {
  try {
    const app = await NestFactory.createApplicationContext(
      CampaignProjectionWorkerModule
    );
    app.enableShutdownHooks();
    console.log('Campaign Projection Worker 부트스트랩 성공');
  } catch (error) {
    console.error('Campaign Projection Worker 부트스트랩 실패:', error);
    process.exit(1);
  }
}

void bootstrap();
