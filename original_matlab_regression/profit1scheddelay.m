function g=profit1scheddelay(p1,p2,f1,f2,M,S1,S2,C1,C2,phi,m,beta,N)
    num=exp(-phi*(f1^-m)+beta*p1);
    denom=exp(-phi*(f1^-m)+beta*p1)+exp(-phi*(f2^-m)+beta*p2)+N;
    ms=num/denom;
    g=C1*f1-min(ms*M,f1*S1)*p1;
