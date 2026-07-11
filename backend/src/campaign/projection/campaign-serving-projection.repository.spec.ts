/* eslint-disable @typescript-eslint/unbound-method */
import type { DataSource } from 'typeorm';
import { CampaignServingProjectionRepository } from './campaign-serving-projection.repository';

describe('CampaignServingProjectionRepository', () => {
  it('loads the projection and outbox watermark from one repeatable-read transaction', async () => {
    const getRawOne = jest.fn().mockResolvedValue({ offset: '41' });
    const find = jest.fn().mockResolvedValue([
      {
        campaignId: 'c1',
        version: '7',
        deleted: false,
        document: { id: 'c1' },
      },
      { campaignId: 'c2', version: '9', deleted: false, document: null },
    ]);
    const manager = {
      query: jest.fn().mockResolvedValue([
        {
          campaignCount: '2',
          projectionCount: '2',
          incompleteRequests: '0',
          unpublishedEvents: '0',
        },
      ]),
      getRepository: jest.fn(() => ({
        createQueryBuilder: jest.fn(() => ({
          select: jest.fn().mockReturnThis(),
          getRawOne,
        })),
        find,
      })),
    };
    const dataSource = {
      transaction: jest.fn(
        (_isolation: string, callback: (value: typeof manager) => unknown) =>
          Promise.resolve(callback(manager))
      ),
    } as unknown as DataSource;
    const repository = new CampaignServingProjectionRepository(dataSource);

    await expect(repository.loadSnapshot()).resolves.toEqual({
      campaigns: [{ id: 'c1' }],
      campaignVersions: new Map([
        ['c1', 7],
        ['c2', 9],
      ]),
      checkpoint: { eventId: 'kafka:0:41', sequence: 42 },
      complete: true,
    });
    expect(dataSource.transaction).toHaveBeenCalledWith(
      'REPEATABLE READ',
      expect.any(Function)
    );
  });

  it('keeps an initial snapshot incomplete while projection work remains', async () => {
    const manager = {
      query: jest.fn().mockResolvedValue([
        {
          campaignCount: '10',
          projectionCount: '9',
          incompleteRequests: '1',
          unpublishedEvents: '0',
        },
      ]),
      getRepository: jest.fn(() => ({
        createQueryBuilder: jest.fn(() => ({
          select: jest.fn().mockReturnThis(),
          getRawOne: jest.fn().mockResolvedValue({ offset: '8' }),
        })),
        find: jest.fn().mockResolvedValue([]),
      })),
    };
    const dataSource = {
      transaction: jest.fn(
        (_isolation: string, callback: (value: typeof manager) => unknown) =>
          Promise.resolve(callback(manager))
      ),
    } as unknown as DataSource;

    await expect(
      new CampaignServingProjectionRepository(dataSource).loadSnapshot()
    ).resolves.toEqual(expect.objectContaining({ complete: false }));
  });
});
