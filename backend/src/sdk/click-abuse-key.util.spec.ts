import { buildClickAbuseDedupKey } from './click-abuse-key.util';

describe('buildClickAbuseDedupKey', () => {
  it('shares a key across auctions for the same visitor, post and intent', () => {
    const input = {
      visitorId: 'visitor-1',
      postUrl: 'https://example.com/post',
      isHighIntent: false,
    };

    expect(buildClickAbuseDedupKey(input)).toBe(buildClickAbuseDedupKey(input));
  });

  it('keeps normal and high-intent click windows independent', () => {
    const base = {
      visitorId: 'visitor-1',
      postUrl: 'https://example.com/post',
    };

    expect(buildClickAbuseDedupKey({ ...base, isHighIntent: false })).not.toBe(
      buildClickAbuseDedupKey({ ...base, isHighIntent: true })
    );
  });
});
