import {getColorForString} from '../../util/getColorForString';

export const Badge = ({text}) => {
  const colors = getColorForString(text);
  return (
    <span
      className={`inline-flex items-center px-2.5 py-0.5 rounded-md text-sm font-medium ${colors}`}
    >
      {text}
    </span>
  );
};

export const Experimental = () => {
  return (
    <div className="experimental-tag">
      <span className="hidden">(</span>Experimental<span className="hidden">)</span>
    </div>
  );
};

export const Deprecated = () => {
  return (
    <div className="deprecated-tag">
      <span className="hidden">(</span>Deprecated<span className="hidden">)</span>
    </div>
  );
};

export const Legacy = () => {
  return (
    <div className="legacy-tag">
      <span className="hidden">(</span>Legacy<span className="hidden">)</span>
    </div>
  );
};
