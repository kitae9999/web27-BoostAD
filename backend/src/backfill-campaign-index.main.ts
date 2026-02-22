import { NestFactory } from '@nestjs/core';
import { BackfillModule } from './backfill/backfill.module';
import { CampaignIndexBackfillService } from './backfill/campaign-index-backfill.service';

async function bootstrap() {
  try {
    const app = await NestFactory.createApplicationContext(BackfillModule);
    app.enableShutdownHooks();

    const backfillService = app.get(CampaignIndexBackfillService);
    await backfillService.run();

    await app.close();
    process.exit(0);
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error('Campaign index backfill 실패:', error);
    process.exit(1);
  }
}

void bootstrap();

