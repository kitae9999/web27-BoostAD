import { getMetadataArgsStorage, Repository } from 'typeorm';
import { BidLog, BidStatus } from '../bid-log.types';
import { BidLogEntity } from '../entities/bid-log.entity';
import { TypeOrmBidLogRepository } from './typeorm-bid-log.repository';

describe('TypeOrmBidLogRepository idempotent writes', () => {
  let typeormRepository: {
    upsert: jest.Mock;
    find: jest.Mock;
  };
  let repository: TypeOrmBidLogRepository;

  const bidLogs: BidLog[] = [
    {
      auctionId: 'auction-1',
      campaignId: 'campaign-a',
      blogId: 7,
      status: BidStatus.WIN,
      bidPrice: 1800,
      reason: '',
      isHighIntent: true,
      behaviorScore: 92,
      postUrl: 'https://blog.example.com/posts/1',
    },
    {
      auctionId: 'auction-1',
      campaignId: 'campaign-b',
      blogId: 7,
      status: BidStatus.LOSS,
      bidPrice: 1500,
      reason: '',
      isHighIntent: true,
      behaviorScore: 92,
      postUrl: 'https://blog.example.com/posts/1',
    },
  ];

  beforeEach(() => {
    typeormRepository = {
      upsert: jest.fn().mockResolvedValue(undefined),
      find: jest.fn(),
    };
    repository = new TypeOrmBidLogRepository(
      typeormRepository as unknown as Repository<BidLogEntity>
    );
  });

  it('declares auction and campaign as a composite unique index', () => {
    const uniqueIndex = getMetadataArgsStorage().indices.find(
      (index) =>
        index.target === BidLogEntity &&
        index.name === 'uq_bidlog_auction_campaign'
    );

    expect(uniqueIndex).toEqual(
      expect.objectContaining({
        columns: ['auctionId', 'campaignId'],
        unique: true,
      })
    );
  });

  it('upserts by auction and campaign and returns rows in input order', async () => {
    const persistedA = { ...bidLogs[0], id: 1 } as BidLogEntity;
    const persistedB = { ...bidLogs[1], id: 2 } as BidLogEntity;
    typeormRepository.find.mockResolvedValue([persistedB, persistedA]);

    await expect(repository.saveMany(bidLogs)).resolves.toEqual([
      persistedA,
      persistedB,
    ]);

    expect(typeormRepository.upsert).toHaveBeenCalledWith(bidLogs, [
      'auctionId',
      'campaignId',
    ]);
    expect(typeormRepository.find).toHaveBeenCalledWith({
      where: [
        { auctionId: 'auction-1', campaignId: 'campaign-a' },
        { auctionId: 'auction-1', campaignId: 'campaign-b' },
      ],
    });
  });

  it('does not write when the batch is empty', async () => {
    await expect(repository.saveMany([])).resolves.toEqual([]);

    expect(typeormRepository.upsert).not.toHaveBeenCalled();
    expect(typeormRepository.find).not.toHaveBeenCalled();
  });
});
