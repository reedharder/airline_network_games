function g=profit_nplayer_scheddelay(p,f,M,S,C,phi,m,beta,N,i,p_i)
    p(i)=p_i; 
    num=exp(-phi*f(i)^(-m)+beta*p(i));
    denom=N+sum(exp(-phi*f.^(-m)+beta*p));
    ms=num/denom;
    g=C(i)*f(i)-min(ms*M,f(i)*S(i))*p(i);
    
    
   