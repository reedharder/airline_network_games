function M = spall_vector_divide(st,t)
    %vector divide as defined in Spall 1998
    % st row vector,t column vector of same length
    M = repmat( st,numel(st),1)./repmat(t,1,numel(t));