function g=profit2scheddelay(p2,p1,f1,f2,M,S1,S2,C1,C2,phi,m,beta,N)
    num=exp(-phi*(f2^-m)+beta*p2);
    denom=exp(-phi*(f1^-m)+beta*p1)+exp(-phi*(f2^-m)+beta*p2)+N;
    ms=num/denom;
    g=C2*f2-min(ms*M,f2*S2)*p2;
