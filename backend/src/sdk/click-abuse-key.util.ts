import { createHash } from 'crypto';

type ClickAbuseKeyInput = {
  visitorId: string;
  postUrl: string;
  isHighIntent: boolean;
};

const digest = (value: string): string =>
  createHash('sha256').update(value).digest('base64url');

/**
 * 기존 View dedup과 같은 정책 경계다.
 * 같은 방문자 + 같은 글 + 같은 intent의 반복 클릭은 시간 창 안에서 한 번만 과금한다.
 */
export const buildClickAbuseDedupKey = ({
  visitorId,
  postUrl,
  isHighIntent,
}: ClickAbuseKeyInput): string => {
  const intent = isHighIntent ? 'high' : 'normal';
  return `dedup:click:window:${intent}:post:${digest(postUrl)}:visitor:${digest(visitorId)}`;
};
